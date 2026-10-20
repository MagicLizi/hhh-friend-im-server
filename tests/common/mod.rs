//! 端到端测试的公共部分，全部在本机：
//!
//! - [`Cindy`]：扮演两个 Cindy 签发方。自己生成一把 RSA 钥匙，用 HTTP 发公钥（JWKS），给测试签身份凭证。
//! - [`Server`]：真的 Friend-IM 服务器（临时数据目录），前面挡一个可以随时“断网”的转发代理 [`Proxy`]，能原地重启。
//! - [`Peer`]：一台电脑 = 真的客户端库 + 它的更新流，`wait` 等到某个更新为止。
//! - [`Raw`]：绕过客户端库直接发协议请求，测服务器对坏请求的拒绝。

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use friend_im_client::core::bytes;
use friend_im_client::core::message::{Content, Part};
use friend_im_client::core::wire::{self, ClientFrame, HelloAuth, Push, Request, Response, ServerFrame, Welcome, WireError};
use friend_im_client::core::keys::RecoveryKey;
use friend_im_client::{Client, ClientError, Config as ClientConfig, Credentials, Message, Update, Updates};
use friend_im_server::config::{Config, Issuer};
use friend_im_server::{App, router};
use futures_util::{SinkExt, StreamExt};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{AbortHandle, JoinHandle};

/// 等一个更新最多等多久。
pub const WAIT: Duration = Duration::from_secs(20);
/// 两个签发方（与真的 Cindy 一样是两个，测试里地址是假的）。
pub const ISSUER_A: &str = "https://cindy-a.test";
pub const ISSUER_B: &str = "https://cindy-b.test";
const KID: &str = "test-key-1";

/// 测试日志：设了 `RUST_LOG` 才输出（服务器和客户端都在同一个进程里）。
pub fn init_logs() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()))
            .try_init();
    });
}

pub fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

struct TestKey {
    der: Vec<u8>,
    n: String,
    e: String,
}

/// 整个测试进程共用一把 RSA 钥匙（生成一把要几百毫秒）。
fn test_key() -> &'static TestKey {
    static KEY: OnceLock<TestKey> = OnceLock::new();
    KEY.get_or_init(|| {
        let key = rsa::RsaPrivateKey::new(&mut rand::rngs::OsRng, 2048).expect("生成 RSA 钥匙");
        TestKey {
            der: key.to_pkcs1_der().expect("写成 DER").as_bytes().to_vec(),
            n: bytes::encode(&key.n().to_bytes_be()),
            e: bytes::encode(&key.e().to_bytes_be()),
        }
    })
}

/// 本进程这把钥匙的公钥集合（JWKS），冒烟测试写给容器里的公钥服务用。
pub fn jwks_json() -> String {
    let key = test_key();
    json!({ "keys": [{ "kty": "RSA", "kid": KID, "use": "sig", "alg": "RS256", "n": key.n, "e": key.e }] }).to_string()
}

/// 假 Cindy。
pub struct Cindy {
    pub issuers: Vec<Issuer>,
}

impl Cindy {
    /// 起一个发公钥的 HTTP 服务。
    pub async fn start() -> Self {
        let jwks = jwks_json();
        let app = axum::Router::new().route(
            "/{issuer}/jwks.json",
            axum::routing::get(move || {
                let body = jwks.clone();
                async move { ([("content-type", "application/json")], body) }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let issuers = [("a", ISSUER_A), ("b", ISSUER_B)]
            .into_iter()
            .map(|(path, issuer)| Issuer {
                issuer: issuer.into(),
                jwks_url: format!("http://{addr}/{path}/jwks.json"),
            })
            .collect();
        Self { issuers }
    }

    /// 一张正常的个人身份凭证（1 小时有效）。
    pub fn token(&self, member: &str, name: &str) -> String {
        self.token_for(ISSUER_A, member, name)
    }

    /// 指定签发方的个人身份凭证。
    pub fn token_for(&self, issuer: &str, member: &str, name: &str) -> String {
        let now = unix_now();
        self.sign(json!({
            "iss": issuer, "aud": "cindy", "sub": member, "typ": "access",
            "iat": now, "exp": now + 3600, "ctx": "personal", "displayName": name,
        }))
    }

    /// 按给定内容签一张凭证（测各种不合规的凭证用）。
    pub fn sign(&self, claims: Value) -> String {
        self.sign_with_kid(claims, KID)
    }

    pub fn sign_with_kid(&self, claims: Value, kid: &str) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.into());
        jsonwebtoken::encode(&header, &claims, &EncodingKey::from_rsa_der(&test_key().der)).unwrap()
    }
}

#[derive(Default)]
struct ProxyState {
    target: Mutex<Option<SocketAddr>>,
    up: AtomicBool,
    conns: Mutex<Vec<AbortHandle>>,
    /// 一次性的“掐断点”：下一条累计传过这么多字节（两个方向合计）的连接被掐断，然后清空。
    trip: Mutex<Option<u64>>,
    tripped: AtomicBool,
}

impl ProxyState {
    /// 这条连接累计传了 `total` 字节：到了掐断点就掐（只掐一次）。
    fn should_cut(&self, total: u64) -> bool {
        let mut trip = self.trip.lock().unwrap();
        if trip.is_some_and(|limit| total > limit) {
            *trip = None;
            self.tripped.store(true, Ordering::SeqCst);
            return true;
        }
        false
    }
}

/// 双向转发，边转边数字节；到了掐断点就整条断开。
async fn relay(inbound: TcpStream, outbound: TcpStream, state: Arc<ProxyState>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let total = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (ri, wi) = inbound.into_split();
    let (ro, wo) = outbound.into_split();
    let pump = |mut from: tokio::net::tcp::OwnedReadHalf, mut to: tokio::net::tcp::OwnedWriteHalf| {
        let total = total.clone();
        let state = state.clone();
        async move {
            let mut buf = vec![0u8; 16 * 1024];
            loop {
                let n = match from.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                let sum = total.fetch_add(n as u64, Ordering::SeqCst) + n as u64;
                if state.should_cut(sum) || to.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        }
    };
    let up = pump(ri, wo);
    let down = pump(ro, wi);
    tokio::select! {
        _ = up => {}
        _ = down => {}
    }
}

/// 转发代理：客户端连它，它再连服务器。`cut` 掐断全部连接并拒绝新连接，模拟断网或服务器停了。
pub struct Proxy {
    pub addr: SocketAddr,
    state: Arc<ProxyState>,
}

impl Proxy {
    async fn start(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(ProxyState::default());
        *state.target.lock().unwrap() = Some(target);
        state.up.store(true, Ordering::SeqCst);
        let shared = state.clone();
        tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                if !shared.up.load(Ordering::SeqCst) {
                    drop(inbound);
                    continue;
                }
                let Some(target) = *shared.target.lock().unwrap() else { continue };
                let relay_state = shared.clone();
                let task = tokio::spawn(async move {
                    if let Ok(outbound) = TcpStream::connect(target).await {
                        relay(inbound, outbound, relay_state).await;
                    }
                });
                shared.conns.lock().unwrap().push(task.abort_handle());
            }
        });
        Self { addr, state }
    }

    /// 断网：掐断现有连接，新连接直接关掉。
    pub fn cut(&self) {
        self.state.up.store(false, Ordering::SeqCst);
        for conn in self.state.conns.lock().unwrap().drain(..) {
            conn.abort();
        }
    }

    /// 恢复。
    pub fn restore(&self) {
        self.state.up.store(true, Ordering::SeqCst);
    }

    /// 下一条累计传过 `bytes` 字节的连接掐断一次（测上传、下载到一半断了）。
    pub fn cut_once_after(&self, bytes: u64) {
        self.state.tripped.store(false, Ordering::SeqCst);
        *self.state.trip.lock().unwrap() = Some(bytes);
    }

    /// 上一次设的掐断点是否已经掐过。
    pub fn tripped(&self) -> bool {
        self.state.tripped.load(Ordering::SeqCst)
    }

    fn retarget(&self, target: SocketAddr) {
        *self.state.target.lock().unwrap() = Some(target);
    }
}

/// 一个 Friend-IM 服务器。
pub struct Server {
    pub dir: tempfile::TempDir,
    pub proxy: Proxy,
    pub direct: SocketAddr,
    task: Option<JoinHandle<()>>,
    config: Config,
}

async fn serve(config: &Config) -> (JoinHandle<()>, SocketAddr) {
    let app = App::new(config).expect("服务器起得来");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, router(app)).await;
    });
    (task, addr)
}

impl Server {
    pub async fn start(cindy: &Cindy) -> Self {
        Self::start_with(cindy, |_| {}).await
    }

    /// 改一下配置再起（如放宽发送频率）。
    pub async fn start_with(cindy: &Cindy, tweak: impl FnOnce(&mut Config)) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::new(dir.path().to_path_buf(), cindy.issuers.clone());
        tweak(&mut config);
        let (task, direct) = serve(&config).await;
        let proxy = Proxy::start(direct).await;
        Self {
            dir,
            proxy,
            direct,
            task: Some(task),
            config,
        }
    }

    /// 客户端用的地址（经过代理）。
    pub fn url(&self) -> String {
        format!("http://{}", self.proxy.addr)
    }

    /// 原地重启：断开全部连接、停掉旧进程，用同一个数据目录起新的，客户端自己重连。
    pub async fn restart(&mut self) {
        self.proxy.cut();
        if let Some(task) = self.task.take() {
            task.abort();
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let (task, direct) = serve(&self.config).await;
        self.task = Some(task);
        self.direct = direct;
        self.proxy.retarget(direct);
        self.proxy.restore();
    }

    /// 直接打开服务器的数据库（查明文、模拟服务器作恶）。
    pub fn db(&self) -> rusqlite::Connection {
        let conn = rusqlite::Connection::open(self.dir.path().join("friend-im.sqlite3")).unwrap();
        conn.busy_timeout(Duration::from_secs(5)).unwrap();
        conn
    }
}

/// 一个测试环境：假 Cindy + 服务器。
pub struct Env {
    pub cindy: Cindy,
    pub server: Server,
}

impl Env {
    /// 测功能用的环境：发送频率放宽（限速单独用 [`Env::with_production_limits`] 测），机器快慢不影响结果。
    pub async fn start() -> Self {
        Self::start_with(|c| {
            c.send_per_second = 10_000;
            c.send_per_minute = 1_000_000;
        })
        .await
    }

    /// 发送频率用正式上限的环境。
    pub async fn with_production_limits() -> Self {
        Self::start_with(|_| {}).await
    }

    pub async fn start_with(tweak: impl FnOnce(&mut Config)) -> Self {
        init_logs();
        let cindy = Cindy::start().await;
        let server = Server::start_with(&cindy, tweak).await;
        Self { cindy, server }
    }

    /// 一台新电脑用 Cindy 凭证第一次登录（还没开通或还没批准）。
    pub async fn device(&self, member: &str, device: &str) -> Peer {
        let token = self.cindy.token(member, member);
        Peer::connect(&self.server.url(), device, Credentials::generate(), Some(token), None)
            .await
            .unwrap_or_else(|e| panic!("{member} 的 {device} 登录失败：{e}"))
    }

    /// 一个开通好的账号（第一台电脑）。
    pub async fn account(&self, member: &str, device: &str) -> (Peer, RecoveryKey) {
        let mut peer = self.device(member, device).await;
        let recovery = peer.client.provision().await.unwrap_or_else(|e| panic!("{member} 开通失败：{e}"));
        peer.wait("开通后可用", |u| matches!(u, Update::Activated).then_some(())).await;
        (peer, recovery)
    }
}

/// 一台电脑。
pub struct Peer {
    pub client: Client,
    pub name: String,
    updates: Updates,
    /// 收到过、还没被 `wait` 认领的更新。
    backlog: Vec<Update>,
    /// 收到过的全部更新（按顺序）。
    pub log: Vec<Update>,
}

impl Peer {
    pub async fn connect(server: &str, name: &str, creds: Credentials, cindy: Option<String>, position: Option<u64>) -> Result<Self, ClientError> {
        let config = ClientConfig {
            server: server.into(),
            device_name: name.into(),
        };
        let (client, updates) = Client::connect(config, creds, cindy, position).await?;
        Ok(Self {
            client,
            name: name.into(),
            updates,
            backlog: Vec::new(),
            log: Vec::new(),
        })
    }

    /// 等到 `f` 认出的那个更新（先看之前收到、没人认领的）。
    pub async fn wait<T>(&mut self, what: &str, mut f: impl FnMut(&Update) -> Option<T>) -> T {
        if let Some(index) = self.backlog.iter().position(|u| f(u).is_some()) {
            let update = self.backlog.remove(index);
            return f(&update).expect("刚认出过");
        }
        let deadline = Instant::now() + WAIT;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(left, self.updates.recv()).await {
                Ok(Some(update)) => {
                    self.log.push(update.clone());
                    if let Some(value) = f(&update) {
                        return value;
                    }
                    self.backlog.push(update);
                }
                Ok(None) => panic!("{} 等{what}时更新流关了；没认领的：{:#?}", self.name, self.backlog),
                Err(_) => panic!("{} 等不到{what}；没认领的：{:#?}", self.name, self.backlog),
            }
        }
    }

    /// 等一会儿把已经到的更新都收进来（之后用 `log` / `backlog` 断言“没有多出来的”）。
    pub async fn settle(&mut self, quiet: Duration) {
        while let Ok(Some(update)) = tokio::time::timeout(quiet, self.updates.recv()).await {
            self.log.push(update.clone());
            self.backlog.push(update);
        }
    }

    /// 等一条文字是 `text` 的消息。
    pub async fn wait_text(&mut self, text: &str) -> Message {
        self.wait(&format!("消息「{text}」"), |u| match u {
            Update::Message(m) if text_of(m).as_deref() == Some(text) => Some(m.clone()),
            _ => None,
        })
        .await
    }

    /// 收到过的全部消息。
    pub fn messages(&self) -> Vec<Message> {
        self.log
            .iter()
            .filter_map(|u| match u {
                Update::Message(m) => Some(m.clone()),
                _ => None,
            })
            .collect()
    }

    /// 最近一次同步位置。
    pub fn position(&self) -> Option<u64> {
        self.log.iter().rev().find_map(|u| match u {
            Update::Position(p) => Some(*p),
            _ => None,
        })
    }
}

/// 一条文字消息。
pub fn text(text: &str) -> Content {
    Content::Post {
        parts: vec![Part::Text { text: text.into() }],
        reply_to: None,
    }
}

/// 消息里的文字（多段拼起来）；不是普通消息为空。
pub fn text_of(message: &Message) -> Option<String> {
    match &message.content {
        Some(Content::Post { parts, .. }) => Some(
            parts
                .iter()
                .filter_map(|p| match p {
                    Part::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect(),
        ),
        _ => None,
    }
}

/// 服务器错误码（不是服务器错误为空）。
pub fn code(error: &ClientError) -> Option<wire::ErrorCode> {
    error.code()
}

/// 绕过客户端库的一条连接：自己登录、自己拼请求。
pub struct Raw {
    socket: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
    next_id: u64,
    pub pushes: Vec<Push>,
}

impl Raw {
    /// 用一台电脑的凭据（登录凭证）登录。
    pub async fn connect(server: &str, creds: &Credentials, name: &str) -> (Self, Welcome) {
        let auth = HelloAuth::Session {
            token: creds.session.clone().expect("有登录凭证"),
        };
        Self::connect_with(server, &creds.device, auth, name).await.expect("登录成功")
    }

    pub async fn connect_with(server: &str, device: &friend_im_client::core::keys::DeviceSecret, auth: HelloAuth, name: &str) -> Result<(Self, Welcome), WireError> {
        let url = format!("{}/v1/ws", server.replacen("http://", "ws://", 1));
        let (socket, _) = tokio_tungstenite::connect_async(url).await.expect("连得上");
        let mut raw = Self {
            socket,
            next_id: 1,
            pushes: Vec::new(),
        };
        let nonce = match raw.frame().await {
            ServerFrame::Challenge { nonce, .. } => nonce,
            other => panic!("第一帧应是随机数：{other:?}"),
        };
        let hello = Request::Hello {
            protocol: wire::PROTOCOL_VERSION,
            auth,
            device: device.public(),
            name: name.into(),
            kind: friend_im_client::core::wire::DeviceKind::Computer,
            sig: wire::sign_hello(device, &nonce),
        };
        match raw.request(hello).await? {
            Response::Welcome { welcome } => Ok((raw, welcome)),
            other => panic!("登录回执不对：{other:?}"),
        }
    }

    async fn frame(&mut self) -> ServerFrame {
        loop {
            let message = tokio::time::timeout(WAIT, self.socket.next()).await.expect("服务器及时回应").expect("连接没断").expect("读得出");
            if let tokio_tungstenite::tungstenite::Message::Text(text) = message {
                return serde_json::from_str(&text).expect("服务器的帧读得懂");
            }
        }
    }

    /// 发一个请求等回执（期间的推送存进 `pushes`）。
    pub async fn request(&mut self, req: Request) -> Result<Response, WireError> {
        let id = self.next_id;
        self.next_id += 1;
        let text = serde_json::to_string(&ClientFrame { id, req }).unwrap();
        self.socket.send(tokio_tungstenite::tungstenite::Message::Text(text.into())).await.expect("发得出");
        loop {
            match self.frame().await {
                ServerFrame::Res { id: got, res } if got == id => return Ok(res),
                ServerFrame::Err { id: got, err } if got == id => return Err(err),
                ServerFrame::Push { push } => self.pushes.push(push),
                _ => {}
            }
        }
    }
}
