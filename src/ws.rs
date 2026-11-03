//! WebSocket 会话：先发一次性随机数，第一个请求必须是 `hello`（登录 + 对随机数的签名），之后逐个处理请求。
//!
//! 一条连接的请求按收到的顺序串行处理；回执和推送走同一个有界队列，由单独的写任务发出。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response as HttpResponse;
use friend_im_core::bytes::Fixed;
use friend_im_core::crypto;
use friend_im_core::limits;
use friend_im_core::wire::{
    self, ClientFrame, ErrorCode, HelloAuth, Push, Request, Response, ServerFrame, WireError,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{Notify, mpsc};

use crate::cindy::CindyError;
use crate::hub::{Outbound, Registration};
use crate::ops_account::{LoginAuth, OpResult, Session, session_hash};
use crate::store::{Effects, Target, fail};
use crate::{App, Locked, now_ms};

/// 连上后多久内必须发 `hello`。
const HELLO_TIMEOUT: Duration = Duration::from_secs(30);
/// 多久没收到任何东西就断开（客户端每 30 秒发一次保活）。
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);

pub(crate) async fn upgrade(State(app): State<Arc<App>>, ws: WebSocketUpgrade) -> HttpResponse {
    ws.max_message_size(limits::FRAME_MAX_BYTES)
        .max_frame_size(limits::FRAME_MAX_BYTES)
        .on_upgrade(move |socket| serve(app, socket))
}

async fn next_text(stream: &mut futures_util::stream::SplitStream<WebSocket>) -> Option<String> {
    loop {
        match stream.next().await? {
            Ok(Message::Text(text)) => return Some(text.to_string()),
            Ok(Message::Ping(_) | Message::Pong(_)) => continue,
            _ => return None,
        }
    }
}

async fn serve(app: Arc<App>, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::channel::<Outbound>(limits::PUSH_QUEUE);
    let kill = Arc::new(Notify::new());
    let relay_bytes = Arc::new(AtomicUsize::new(0));
    let queued = relay_bytes.clone();
    let writer = tokio::spawn(async move {
        while let Some(out) = rx.recv().await {
            let (frame, relayed) = match out {
                Outbound::Frame(frame) => (frame, 0),
                Outbound::Relay(frame, size) => (frame, size),
                Outbound::Close => {
                    let _ = sink.send(Message::Close(None)).await;
                    break;
                }
            };
            let Ok(text) = serde_json::to_string(&frame) else {
                continue;
            };
            let sent = sink.send(Message::Text(text.into())).await;
            queued.fetch_sub(relayed, Ordering::Relaxed);
            if sent.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });

    let nonce = Fixed(crypto::random::<32>());
    let challenge = ServerFrame::Challenge {
        protocol: wire::PROTOCOL_VERSION,
        nonce,
        server_time_ms: now_ms(),
    };
    if tx.send(Outbound::Frame(Box::new(challenge))).await.is_err() {
        return;
    }
    let registration =
        match tokio::time::timeout(
            HELLO_TIMEOUT,
            hello(&app, &mut stream, &tx, &kill, &relay_bytes, &nonce),
        )
        .await
        {
            Ok(Ok(registration)) => registration,
            Ok(Err((id, error))) => {
                tracing::info!(code = ?error.code, "登录失败");
                let _ = tx
                    .send(Outbound::Frame(Box::new(ServerFrame::Err {
                        id,
                        err: error,
                    })))
                    .await;
                let _ = tx.send(Outbound::Close).await;
                drop(tx);
                let _ = writer.await;
                return;
            }
            Err(_) => {
                let _ = tx.send(Outbound::Close).await;
                drop(tx);
                let _ = writer.await;
                return;
            }
        };
    let session = Session {
        uid: registration.uid.clone(),
        device: registration.device.clone(),
    };
    drop(tx);
    let registration = Arc::new(registration);
    if registration.approved {
        announce(&app, session.clone(), true).await;
    }
    loop {
        let text = tokio::select! {
            _ = kill.notified() => break,
            next = tokio::time::timeout(IDLE_TIMEOUT, next_text(&mut stream)) => match next {
                Ok(Some(text)) => text,
                _ => break,
            },
        };
        let Ok(frame) = serde_json::from_str::<ClientFrame>(&text) else {
            tracing::info!("读不懂的请求，断开");
            break;
        };
        if matches!(frame.req, Request::Hello { .. }) {
            app.hub.reply(
                &registration,
                err_frame(frame.id, fail(ErrorCode::BadRequest, "已经登录过了")),
            );
            continue;
        }
        dispatch(&app, &session, &registration, frame).await;
    }
    if app.hub.unregister(&registration) {
        announce(&app, session.clone(), false).await;
    }
    let _ = writer.await;
    tracing::debug!(device = %session.device, "连接断开");
}

/// 告诉能和这台设备对账的设备：它上线或下线了。查不到也没关系，对方下次兜底对账时一样补齐。
async fn announce(app: &Arc<App>, session: Session, online: bool) {
    app.with_store(move |locked, hub| {
        let peers = match locked.store.peers_of(&session.uid) {
            Ok(peers) => peers,
            Err(error) => {
                tracing::warn!(?error, "查不到这台设备的对账伙伴");
                return;
            }
        };
        let push = Push::Presence {
            uid: session.uid.clone(),
            device: session.device.clone(),
            online,
        };
        let mut effects = Effects::default();
        for uid in peers {
            let target = Target::AccountExcept(uid, vec![session.device.clone()]);
            effects.push(target, push.clone());
        }
        hub.apply(&effects, None);
    })
    .await;
}

fn err_frame(id: u64, err: WireError) -> ServerFrame {
    ServerFrame::Err { id, err }
}

async fn hello(
    app: &Arc<App>,
    stream: &mut futures_util::stream::SplitStream<WebSocket>,
    tx: &mpsc::Sender<Outbound>,
    kill: &Arc<Notify>,
    relay_bytes: &Arc<AtomicUsize>,
    nonce: &Fixed<32>,
) -> Result<Registration, (u64, WireError)> {
    let text = next_text(stream)
        .await
        .ok_or((0, fail(ErrorCode::BadRequest, "连接断开")))?;
    let frame: ClientFrame = serde_json::from_str(&text)
        .map_err(|_| (0, fail(ErrorCode::BadRequest, "第一个请求读不懂")))?;
    let id = frame.id;
    let Request::Hello {
        protocol,
        auth,
        device,
        name,
        kind,
        sig,
    } = frame.req
    else {
        return Err((id, fail(ErrorCode::Unauthorized, "第一个请求必须是 hello")));
    };
    if protocol != wire::PROTOCOL_VERSION {
        return Err((
            id,
            fail(
                ErrorCode::Unsupported,
                format!("服务器只支持接口版本 {}", wire::PROTOCOL_VERSION),
            ),
        ));
    }
    wire::check_hello(&device, nonce, &sig)
        .map_err(|_| (id, fail(ErrorCode::Unauthorized, "电脑签名不对")))?;
    let login = match auth {
        HelloAuth::Cindy { token } => match app.verifier.verify(&token).await {
            Ok(identity) => LoginAuth::Cindy(identity),
            Err(CindyError::KeysUnavailable) => {
                return Err((
                    id,
                    fail(ErrorCode::Internal, "暂时取不到 Cindy 的公钥，请稍后再试"),
                ));
            }
            Err(CindyError::Expired) => {
                return Err((
                    id,
                    fail(ErrorCode::Expired, "Cindy 凭证已过期，请重新登录 Cindy"),
                ));
            }
            Err(CindyError::Invalid) => {
                return Err((id, fail(ErrorCode::Unauthorized, "Cindy 凭证无效")));
            }
        },
        HelloAuth::Session { token } => LoginAuth::Session(session_hash(&token.0)),
    };
    let tx = tx.clone();
    let kill = kill.clone();
    let relay_bytes = relay_bytes.clone();
    app.with_store(move |locked, hub| {
        let (session, welcome, effects) =
            locked.store.login(login, device, &name, kind, now_ms())?;
        let approved = welcome.status == wire::DeviceStatus::Active;
        let registration =
            hub.register(&session.uid, &session.device, approved, tx, kill, relay_bytes);
        hub.reply(
            &registration,
            ServerFrame::Res {
                id,
                res: Response::Welcome { welcome },
            },
        );
        hub.apply(&effects, Some(&registration));
        tracing::info!(device = %session.device, ?approved, "电脑已连接");
        Ok(registration)
    })
    .await
    .map_err(|error: WireError| (id, error))
}

/// 在存储锁里执行一个请求：事务提交 → 推送入队 → 回执入队 →（需要时）关闭本连接，顺序固定。
async fn dispatch(
    app: &Arc<App>,
    session: &Session,
    registration: &Arc<Registration>,
    frame: ClientFrame,
) {
    let session = session.clone();
    let registration = registration.clone();
    app.with_store(move |locked, hub| {
        let id = frame.id;
        let now = now_ms();
        let friends = matches!(
            &frame.req,
            Request::FriendRequests { .. }
                | Request::FriendRequestGet { .. }
                | Request::FriendRequestSend { .. }
        );
        let result = run(locked, hub, &session, frame.req, now);
        match result {
            Ok((response, effects)) => {
                if friends {
                    hub.enable_friends(&registration);
                }
                let close_self = hub.apply(&effects, Some(&registration));
                hub.reply(&registration, ServerFrame::Res { id, res: response });
                if close_self {
                    hub.close(&registration);
                }
            }
            Err(error) => {
                if error.code == ErrorCode::Internal {
                    tracing::warn!(device = %session.device, "请求失败：服务器内部错误");
                } else {
                    tracing::debug!(device = %session.device, code = ?error.code, "请求被拒");
                }
                hub.reply(&registration, err_frame(id, error));
            }
        }
    })
    .await;
}

fn run(
    locked: &mut Locked,
    hub: &crate::hub::Hub,
    s: &Session,
    req: Request,
    now: i64,
) -> OpResult {
    let store = &mut locked.store;
    match req {
        Request::Hello { .. } => Err(fail(ErrorCode::BadRequest, "已经登录过了")),
        Request::Ping => Ok((Response::Pong, Default::default())),
        Request::FriendRequestSend { envelope } => store.friend_request_send(s, &envelope, now),
        Request::FriendRequests {
            before,
            limit,
            incoming,
            peer,
        } => store.friend_requests(s, before, limit, incoming, peer.as_ref(), now),
        Request::FriendRequestGet { request } => store.friend_request_get(s, &request, now),
        Request::FriendRequestAccept {
            request,
            link,
            keys,
        } => store.friend_request_accept(s, &request, &link, &keys, now),
        Request::FriendRequestReject { request } => {
            store.friend_request_close(s, &request, false, now)
        }
        Request::FriendRequestWithdraw { request } => {
            store.friend_request_close(s, &request, true, now)
        }
        Request::GetChain { uid, epoch, after } => store.get_chain(s, &uid, epoch, after),
        Request::Provision { eldest, keys } => store.provision(s, &eldest, &keys, now),
        Request::AppendAccount { links, keys } => {
            store.append_account(s, &links, keys.as_ref(), now)
        }
        Request::Reset { eldest, keys } => store.reset(s, &eldest, &keys, now),
        Request::GetAccountKeys => store.get_account_keys(s),
        Request::GetRecoveryKeys => store.get_recovery_keys(s),
        Request::ListDevices => store.list_devices(s, &|d| hub.is_online(d)),
        Request::JoinStart { pake, proof } => {
            store.join_start(s, &pake, &proof, &|d| hub.is_online(d), now)
        }
        Request::JoinRetry { request, pake } => store.join_retry(s, &request, &pake, now),
        Request::JoinConfirm {
            request,
            attempt,
            confirm,
        } => store.join_confirm(s, &request, attempt, &confirm, now),
        Request::JoinCancel { request } => store.join_cancel(s, &request, now),
        Request::JoinRespond {
            request,
            attempt,
            pake,
            confirm,
        } => store.join_respond(s, &request, attempt, &pake, &confirm, now),
        Request::JoinApprove {
            request,
            link,
            keys,
        } => store.join_approve(s, &request, &link, &keys, now),
        Request::JoinReject { request } => store.join_reject(s, &request, now),
        Request::ListJoinRequests => store.list_join_requests(s, now),
        Request::JoinWithRecovery { link, keys } => store.join_with_recovery(s, &link, &keys, now),
        Request::GetFriendLink => store.get_friend_link(s, false),
        Request::RotateFriendLink => store.get_friend_link(s, true),
        Request::ResolveFriendLink { token } => store.resolve_friend_link(s, &token),
        Request::GetProfiles { uids } => store.get_profiles(s, &uids),
        Request::CreateConv { link, keys } => store.create_conv(s, &link, &keys, now),
        Request::AppendConv {
            conv,
            links,
            keys,
            invite,
        } => store.append_conv(s, &conv, &links, keys.as_ref(), invite.as_ref(), now),
        Request::GrantConvKeys { conv, keys } => store.grant_conv_keys(s, &conv, &keys, now),
        Request::GetConv { conv, after } => store.get_conv(s, &conv, after),
        Request::GetConvKeys { conv } => store.get_conv_keys(s, &conv),
        Request::ListConvs => store.list_convs(s),
        Request::GetInvite { invite } => store.get_invite(s, &invite, now),
        Request::JoinByInvite { conv, link, keys } => {
            store.join_by_invite(s, &conv, &link, &keys, now)
        }
        Request::Relay { to, conv, frame } => {
            store.relay_check(s, &to, conv.as_ref(), &frame, &mut locked.limiter, now)?;
            let size = frame.sealed.ct.len();
            let online = hub.relay(&to, Push::Relayed { conv, frame }, size);
            Ok((Response::Relayed { online }, Default::default()))
        }
        Request::Peers => store.online_peers(s, &|uid| hub.online_devices(uid)),
        Request::BeforeSend { conv, generation } => store.before_send(s, &conv, generation),
        Request::Sync { since, limit } => store.sync(s, since, limit),
    }
}
