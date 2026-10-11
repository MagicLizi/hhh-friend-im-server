//! 测试里的“HHH”：在进程内起一个真的 Friend-IM 插件（`friend_im::serve`，和程序走同一套代码），用管道和它讲协议。
//!
//! - 插件写出的每一行都按 JSON-RPC 解析，解不开的记下来（标准输出只能有协议消息，§3）；
//! - 插件发来的请求：`events/push` 默认收下（可以改成拒收，测重推），`credential/save` 收下并记住；
//! - 插件发来的通知（`status`、`session/expired`、`login/failed`、`log/write`）全部记下；
//! - `call` 发请求等回复；`wait` 等到插件发来的某样东西为止。

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use friend_im::Options;
use hhh_im_protocol::method::EventsPushParams;
use hhh_im_protocol::rpc::{RpcError, RpcId, RpcMessage, codec};
use hhh_im_protocol::types::{Block, Event, Message};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;

type Writer = Arc<tokio::sync::Mutex<Box<dyn AsyncWrite + Send + Unpin>>>;

/// 等插件做完一件事最多等多久。
pub const WAIT: Duration = Duration::from_secs(20);

/// 插件发给“HHH”的一样东西。
#[derive(Clone, Debug)]
pub enum Seen {
    /// 一批推送（收下与否以当时的设置为准）。
    Push(EventsPushParams),
    /// 存凭据。
    Save(String),
    /// 通知：方法名与参数。
    Note(String, Value),
}

/// 一个插件进程（在测试进程里）和它对面的“HHH”。
pub struct Host {
    writer: Writer,
    /// 真的插件程序（[`Host::spawn`] 起的）；进程内的为空。
    pub child: Option<tokio::process::Child>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<RpcMessage>>>>,
    pub seen: Arc<Mutex<Vec<Seen>>>,
    /// 插件写出的、不是 JSON-RPC 的行。
    pub bad_lines: Arc<Mutex<Vec<String>>>,
    /// 收不收推送（假时回错误，插件应当重推）。
    pub accept_pushes: Arc<AtomicBool>,
    next: AtomicI64,
}

pub fn options() -> Options {
    Options { push_retry: Duration::from_millis(100), send_wait: Duration::from_secs(10) }
}

impl Host {
    /// 起一个插件，握手（`initialize`），返回握手结果。
    pub async fn start(temp_dir: &std::path::Path) -> (Self, Value) {
        Self::start_with(temp_dir, options()).await
    }

    pub async fn start_with(temp_dir: &std::path::Path, options: Options) -> (Self, Value) {
        let (to_plugin, plugin_in) = tokio::io::duplex(8 << 20);
        let (plugin_out, from_plugin) = tokio::io::duplex(8 << 20);
        tokio::spawn(friend_im::serve(plugin_in, plugin_out, options));
        Self::connect(Box::new(to_plugin), from_plugin, None, temp_dir).await
    }

    /// 起真的插件程序（子进程，标准输入输出讲协议），握手。
    pub async fn spawn(program: &std::path::Path, temp_dir: &std::path::Path) -> (Self, Value) {
        let mut child = tokio::process::Command::new(program)
            .env("HHH_IM_PROTOCOL", "1")
            .current_dir(temp_dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap_or_else(|e| panic!("起不了插件程序 {}：{e}", program.display()));
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        Self::connect(Box::new(stdin), stdout, Some(child), temp_dir).await
    }

    async fn connect(writer: Box<dyn AsyncWrite + Send + Unpin>, reader: impl AsyncRead + Unpin + Send + 'static, child: Option<tokio::process::Child>, temp_dir: &std::path::Path) -> (Self, Value) {
        let host = Self {
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            child,
            pending: Arc::default(),
            seen: Arc::default(),
            bad_lines: Arc::default(),
            accept_pushes: Arc::new(AtomicBool::new(true)),
            next: AtomicI64::new(1),
        };
        tokio::spawn(read_loop(
            reader,
            host.writer.clone(),
            host.pending.clone(),
            host.seen.clone(),
            host.bad_lines.clone(),
            host.accept_pushes.clone(),
        ));
        let init = host
            .call(
                "initialize",
                json!({ "protocolVersions": [1], "host": { "name": "HHH", "version": "test" }, "locale": "zh-CN",
                        "tempDir": temp_dir.to_string_lossy(), "network": {} }),
            )
            .await
            .expect("握手");
        (host, init)
    }

    async fn write(&self, message: &RpcMessage) {
        let line = codec::encode_line(message).expect("写得出");
        let mut writer = self.writer.lock().await;
        writer.write_all(line.as_bytes()).await.expect("插件的输入还开着");
        writer.flush().await.unwrap();
    }

    /// 发一个请求，等回复。
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(RpcId::Number(id).to_string(), tx);
        self.write(&RpcMessage::request(RpcId::Number(id), method, params)).await;
        let reply = tokio::time::timeout(Duration::from_secs(70), rx).await.unwrap_or_else(|_| panic!("{method} 没回复")).expect("回复");
        match (reply.result, reply.error) {
            (_, Some(error)) => Err(error),
            (Some(result), None) => Ok(result),
            (None, None) => Ok(Value::Null),
        }
    }

    /// 发一个请求，隔 `after` 发 `$/cancel` 取消它，返回插件的回复与取消后多久回的。
    pub async fn call_then_cancel(&self, method: &str, params: Value, after: Duration) -> (Result<Value, RpcError>, Duration) {
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(RpcId::Number(id).to_string(), tx);
        self.write(&RpcMessage::request(RpcId::Number(id), method, params)).await;
        tokio::time::sleep(after).await;
        let cancelled = Instant::now();
        self.notify("$/cancel", json!({ "id": id })).await;
        let reply = tokio::time::timeout(Duration::from_secs(10), rx).await.unwrap_or_else(|_| panic!("取消后 {method} 没回复")).expect("回复");
        let result = match (reply.result, reply.error) {
            (_, Some(error)) => Err(error),
            (Some(result), None) => Ok(result),
            (None, None) => Ok(Value::Null),
        };
        (result, cancelled.elapsed())
    }

    /// 发一个通知。
    pub async fn notify(&self, method: &str, params: Value) {
        self.write(&RpcMessage::notification(method, params)).await;
    }

    /// 用凭据上线（`session/open`），返回结果。
    pub async fn open(&self, server: &str, credential: Option<&str>, sync_state: Option<&str>) -> Value {
        self.call("session/open", json!({ "credential": credential, "settings": { "serverUrl": server }, "syncState": sync_state }))
            .await
            .expect("session/open")
    }

    /// 关掉插件的标准输入（插件下线、退出）。子进程的管道 `shutdown` 不会关，要把这一头扔掉才关。
    pub async fn close(&self) {
        let mut writer = self.writer.lock().await;
        let _ = writer.shutdown().await;
        *writer = Box::new(tokio::io::sink());
    }

    /// 等到 `f` 在插件发来的东西里认出要的东西。
    pub async fn wait<T>(&self, what: &str, f: impl Fn(&[Seen]) -> Option<T>) -> T {
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(found) = f(&self.seen.lock().unwrap()) {
                return found;
            }
            assert!(Instant::now() < deadline, "等不到{what}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// 推来过的全部消息（按推来的先后；重推的会重复出现）。
    pub fn pushed_messages(&self) -> Vec<Message> {
        events(&self.seen.lock().unwrap())
            .into_iter()
            .filter_map(|e| match e {
                Event::Messages { messages, .. } => Some(messages),
                _ => None,
            })
            .flatten()
            .collect()
    }

    /// 等推来一条文字是 `text` 的消息。
    pub async fn wait_text(&self, text: &str) -> Message {
        self.wait(&format!("推来消息「{text}」"), |seen| {
            events(seen).into_iter().find_map(|e| match e {
                Event::Messages { messages, .. } => messages.into_iter().find(|m| text_of(m) == text),
                _ => None,
            })
        })
        .await
    }

    /// 等推来编号是 `id` 的消息里第一条满足 `f` 的。
    pub async fn wait_message(&self, what: &str, f: impl Fn(&Message) -> bool) -> Message {
        self.wait(what, |seen| {
            events(seen).into_iter().find_map(|e| match e {
                Event::Messages { messages, .. } => messages.into_iter().find(|m| f(m)),
                _ => None,
            })
        })
        .await
    }

    /// 等一个通知。
    pub async fn wait_note(&self, method: &str, f: impl Fn(&Value) -> bool) -> Value {
        self.wait(&format!("通知 {method}"), |seen| {
            seen.iter().find_map(|s| match s {
                Seen::Note(m, params) if m == method && f(params) => Some(params.clone()),
                _ => None,
            })
        })
        .await
    }

    /// 最后一次推送带的同步位置。
    pub fn last_sync_state(&self) -> Option<String> {
        self.seen.lock().unwrap().iter().rev().find_map(|s| match s {
            Seen::Push(p) => p.sync_state.clone(),
            _ => None,
        })
    }

    /// 插件交给宿主的全部日志（`log/write`）与通知拼成一段文字：查里面有没有消息正文。
    pub fn notes_text(&self) -> String {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter_map(|s| match s {
                Seen::Note(m, params) => Some(format!("{m} {params}")),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// 真的插件程序：在插件仓库里编一次（和这个测试同一种构建：调试或发布），返回程序的路径。
pub fn program() -> std::path::PathBuf {
    static BUILT: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    BUILT
        .get_or_init(|| {
            let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../hhh-friend-im");
            let release = !cfg!(debug_assertions);
            let mut cargo = std::process::Command::new(env!("CARGO"));
            cargo.current_dir(&repo).args(["build", "--bin", "friend-im"]).env_remove("CARGO_TARGET_DIR");
            if release {
                cargo.arg("--release");
            }
            let status = cargo.status().expect("起得了 cargo");
            assert!(status.success(), "插件程序编不过");
            repo.join("target").join(if release { "release" } else { "debug" }).join(format!("friend-im{}", std::env::consts::EXE_SUFFIX))
        })
        .clone()
}

/// 一个进程现在占的内存（常驻集，字节）。
pub fn resident_bytes(pid: u32) -> u64 {
    #[cfg(windows)]
    let output = std::process::Command::new("powershell")
        .args(["-NoProfile", "-Command", &format!("(Get-Process -Id {pid}).WorkingSet64")])
        .output()
        .expect("起得了 powershell");
    #[cfg(not(windows))]
    let output = std::process::Command::new("ps").args(["-o", "rss=", "-p", &pid.to_string()]).output().expect("起得了 ps");
    let number: u64 = String::from_utf8_lossy(&output.stdout).trim().parse().expect("读得出内存");
    if cfg!(windows) { number } else { number * 1024 }
}

/// 推送里的全部事件（按先后）。
pub fn events(seen: &[Seen]) -> Vec<Event> {
    seen.iter()
        .filter_map(|s| match s {
            Seen::Push(p) => Some(p.events.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// 消息里的文字（文字块连起来）。
pub fn text_of(message: &Message) -> String {
    message
        .content
        .iter()
        .filter_map(|b| match b {
            Block::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

async fn read_loop(
    from_plugin: impl AsyncRead + Unpin,
    writer: Writer,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<RpcMessage>>>>,
    seen: Arc<Mutex<Vec<Seen>>>,
    bad_lines: Arc<Mutex<Vec<String>>>,
    accept: Arc<AtomicBool>,
) {
    let mut lines = BufReader::new(from_plugin).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let message = match codec::decode_line(&line) {
            Ok(message) => message,
            Err(_) => {
                bad_lines.lock().unwrap().push(line);
                continue;
            }
        };
        if message.is_response() {
            let id = message.id.as_ref().map(ToString::to_string).unwrap_or_default();
            if let Some(tx) = pending.lock().unwrap().remove(&id) {
                let _ = tx.send(message);
            }
            continue;
        }
        let method = message.method.clone().unwrap_or_default();
        let params = message.params.clone().unwrap_or(Value::Null);
        let Some(id) = message.id.clone() else {
            seen.lock().unwrap().push(Seen::Note(method, params));
            continue;
        };
        let reply = match method.as_str() {
            "events/push" => {
                let push: EventsPushParams = serde_json::from_value(params).expect("推送的参数合协议");
                seen.lock().unwrap().push(Seen::Push(push));
                if accept.load(Ordering::SeqCst) {
                    RpcMessage::ok(id, json!({}))
                } else {
                    RpcMessage::err(id, RpcError::from_kind(hhh_im_protocol::ErrorKind::Disconnected, "测试：这批先不收"))
                }
            }
            "credential/save" => {
                seen.lock().unwrap().push(Seen::Save(params["credential"].as_str().unwrap_or_default().to_owned()));
                RpcMessage::ok(id, json!({}))
            }
            other => RpcMessage::err(id, RpcError::from_kind(hhh_im_protocol::ErrorKind::Unsupported, format!("测试宿主不认识 {other}"))),
        };
        let line = codec::encode_line(&reply).unwrap();
        let mut writer = writer.lock().await;
        if writer.write_all(line.as_bytes()).await.is_err() {
            break;
        }
        let _ = writer.flush().await;
    }
}
