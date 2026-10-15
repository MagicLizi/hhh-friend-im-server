//! WebSocket 会话：先发一次性随机数，第一个请求必须是 `hello`（登录 + 对随机数的签名），之后逐个处理请求。
//!
//! 一条连接的请求按收到的顺序串行处理；回执和推送走同一个有界队列，由单独的写任务发出。

use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response as HttpResponse;
use friend_im_core::bytes::Fixed;
use friend_im_core::crypto;
use friend_im_core::limits;
use friend_im_core::wire::{self, ClientFrame, ErrorCode, HelloAuth, Request, Response, ServerFrame, WireError};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{Notify, mpsc};

use crate::cindy::CindyError;
use crate::hub::{Outbound, Registration};
use crate::ops_account::{LoginAuth, OpResult, Session, session_hash};
use crate::store::fail;
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
    let writer = tokio::spawn(async move {
        while let Some(out) = rx.recv().await {
            match out {
                Outbound::Frame(frame) => {
                    let Ok(text) = serde_json::to_string(&frame) else { continue };
                    if sink.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                Outbound::Close => {
                    let _ = sink.send(Message::Close(None)).await;
                    break;
                }
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
    let registration = match tokio::time::timeout(HELLO_TIMEOUT, hello(&app, &mut stream, &tx, &kill, &nonce)).await {
        Ok(Ok(registration)) => registration,
        Ok(Err((id, error))) => {
            tracing::info!(code = ?error.code, "登录失败");
            let _ = tx.send(Outbound::Frame(Box::new(ServerFrame::Err { id, err: error }))).await;
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
            app.hub.reply(&registration, err_frame(frame.id, fail(ErrorCode::BadRequest, "已经登录过了")));
            continue;
        }
        dispatch(&app, &session, &registration, frame).await;
    }
    app.hub.unregister(&registration);
    let _ = writer.await;
    tracing::debug!(device = %session.device, "连接断开");
}

fn err_frame(id: u64, err: WireError) -> ServerFrame {
    ServerFrame::Err { id, err }
}

async fn hello(
    app: &Arc<App>,
    stream: &mut futures_util::stream::SplitStream<WebSocket>,
    tx: &mpsc::Sender<Outbound>,
    kill: &Arc<Notify>,
    nonce: &Fixed<32>,
) -> Result<Registration, (u64, WireError)> {
    let text = next_text(stream).await.ok_or((0, fail(ErrorCode::BadRequest, "连接断开")))?;
    let frame: ClientFrame = serde_json::from_str(&text).map_err(|_| (0, fail(ErrorCode::BadRequest, "第一个请求读不懂")))?;
    let id = frame.id;
    let Request::Hello {
        protocol,
        auth,
        device,
        name,
        sig,
    } = frame.req
    else {
        return Err((id, fail(ErrorCode::Unauthorized, "第一个请求必须是 hello")));
    };
    if protocol != wire::PROTOCOL_VERSION {
        return Err((id, fail(ErrorCode::Unsupported, format!("服务器只支持接口版本 {}", wire::PROTOCOL_VERSION))));
    }
    wire::check_hello(&device, nonce, &sig).map_err(|_| (id, fail(ErrorCode::Unauthorized, "电脑签名不对")))?;
    let login = match auth {
        HelloAuth::Cindy { token } => match app.verifier.verify(&token).await {
            Ok(identity) => LoginAuth::Cindy(identity),
            Err(CindyError::KeysUnavailable) => return Err((id, fail(ErrorCode::Internal, "暂时取不到 Cindy 的公钥，请稍后再试"))),
            Err(CindyError::Expired) => return Err((id, fail(ErrorCode::Expired, "Cindy 凭证已过期，请重新登录 Cindy"))),
            Err(CindyError::Invalid) => return Err((id, fail(ErrorCode::Unauthorized, "Cindy 凭证无效"))),
        },
        HelloAuth::Session { token } => LoginAuth::Session(session_hash(&token.0)),
    };
    let tx = tx.clone();
    let kill = kill.clone();
    app.with_store(move |locked, hub| {
        let (session, welcome, effects) = locked.store.login(login, device, &name, now_ms())?;
        let approved = welcome.status == wire::DeviceStatus::Active;
        let registration = hub.register(&session.uid, &session.device, approved, tx, kill);
        hub.reply(&registration, ServerFrame::Res {
            id,
            res: Response::Welcome { welcome },
        });
        hub.apply(&effects, Some(&registration));
        tracing::info!(device = %session.device, ?approved, "电脑已连接");
        Ok(registration)
    })
    .await
    .map_err(|error: WireError| (id, error))
}

/// 在存储锁里执行一个请求：事务提交 → 推送入队 → 回执入队 →（需要时）关闭本连接，顺序固定。
async fn dispatch(app: &Arc<App>, session: &Session, registration: &Arc<Registration>, frame: ClientFrame) {
    let session = session.clone();
    let registration = registration.clone();
    app.with_store(move |locked, hub| {
        let id = frame.id;
        let now = now_ms();
        let result = run(locked, hub, &session, frame.req, now);
        match result {
            Ok((response, effects)) => {
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

fn run(locked: &mut Locked, hub: &crate::hub::Hub, s: &Session, req: Request, now: i64) -> OpResult {
    let store = &mut locked.store;
    match req {
        Request::Hello { .. } => Err(fail(ErrorCode::BadRequest, "已经登录过了")),
        Request::Ping => Ok((Response::Pong, Default::default())),
        Request::GetChain { uid, epoch, after } => store.get_chain(s, &uid, epoch, after),
        Request::Provision { eldest, keys } => store.provision(s, &eldest, &keys, now),
        Request::AppendAccount { links, keys } => store.append_account(s, &links, keys.as_ref(), now),
        Request::Reset { eldest, keys } => store.reset(s, &eldest, &keys, now),
        Request::GetAccountKeys => store.get_account_keys(s),
        Request::GetRecoveryKeys => store.get_recovery_keys(s),
        Request::ListDevices => store.list_devices(s, &|d| hub.is_online(d)),
        Request::JoinStart { pake, proof } => store.join_start(s, &pake, &proof, &|d| hub.is_online(d), now),
        Request::JoinRetry { request, pake } => store.join_retry(s, &request, &pake, now),
        Request::JoinConfirm { request, attempt, confirm } => store.join_confirm(s, &request, attempt, &confirm, now),
        Request::JoinCancel { request } => store.join_cancel(s, &request, now),
        Request::JoinRespond {
            request,
            attempt,
            pake,
            confirm,
        } => store.join_respond(s, &request, attempt, &pake, &confirm, now),
        Request::JoinApprove { request, link, keys } => store.join_approve(s, &request, &link, &keys, now),
        Request::JoinReject { request } => store.join_reject(s, &request, now),
        Request::ListJoinRequests => store.list_join_requests(s, now),
        Request::JoinWithRecovery { link, keys } => store.join_with_recovery(s, &link, &keys, now),
        Request::GetFriendLink => store.get_friend_link(s, false),
        Request::RotateFriendLink => store.get_friend_link(s, true),
        Request::ResolveFriendLink { token } => store.resolve_friend_link(s, &token),
        Request::GetProfiles { uids } => store.get_profiles(s, &uids),
        Request::CreateConv { link, keys } => store.create_conv(s, &link, &keys, now),
        Request::AppendConv { conv, links, keys, invite } => store.append_conv(s, &conv, &links, keys.as_ref(), invite.as_ref(), now),
        Request::GrantConvKeys { conv, keys } => store.grant_conv_keys(s, &conv, &keys, now),
        Request::GetConv { conv, after } => store.get_conv(s, &conv, after),
        Request::GetConvKeys { conv } => store.get_conv_keys(s, &conv),
        Request::ListConvs => store.list_convs(s),
        Request::GetInvite { invite } => store.get_invite(s, &invite, now),
        Request::JoinByInvite { conv, link, keys } => store.join_by_invite(s, &conv, &link, &keys, now),
        Request::Send { env } => store.send(s, &env, &mut locked.limiter, now, true),
        Request::SendLan { env } => store.send(s, &env, &mut locked.limiter, now, false),
        Request::ReleaseMessage { conv, id, direct_devices } => store.release_message(s, &conv, &id, &direct_devices),
        Request::History { conv, before, limit, since_ms, until_ms } => store.history(s, &conv, before, limit, since_ms, until_ms),
        Request::Activity { since_ms } => store.activity(s, since_ms),
        Request::Sync { since, limit } => store.sync(s, since, limit),
        Request::FileCreate { conv, size } => store.file_create(s, &conv, size, now),
    }
}
