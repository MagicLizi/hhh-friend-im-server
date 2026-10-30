//! Friend-IM 服务器：账号、电脑、好友、群、密文消息与文件的存储和转发。只存密文，解不开。
//!
//! - `GET /v1/ws`：每台电脑一条 WebSocket 长连接（请求 / 回执 / 推送，格式见 `friend_im_core::wire`）。
//! - `PUT` / `HEAD` / `GET /v1/files/{编号}`：文件按段上传、断点续传、按范围下载（只有密文）。
//! - `GET /healthz`：健康检查。

pub mod cindy;
pub mod config;
mod files;
pub mod hub;
mod ops_account;
mod ops_conv;
mod ops_friends;
mod ops_join;
mod ops_msg;
mod ops_reading;
pub mod store;
mod ws;

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::get;

pub use ops_account::{LoginAuth, Session};
use ops_msg::{RateLimiter, Uploads};

/// 存储锁里的全部可变状态：所有写入串行执行，推送在锁里按日志顺序入队。
pub struct Locked {
    pub store: store::Store,
    pub(crate) limiter: RateLimiter,
    pub(crate) reading_limiter: RateLimiter,
    pub(crate) uploads: Uploads,
}

/// 服务器的共享状态。
pub struct App {
    pub(crate) locked: Mutex<Locked>,
    pub(crate) hub: hub::Hub,
    pub(crate) verifier: cindy::Verifier,
}

/// 现在的毫秒时间。
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl App {
    /// 用配置打开存储、建好共享状态。
    pub fn new(config: &config::Config) -> Result<Arc<Self>, String> {
        let store = store::Store::open(&config.data_dir)?;
        Ok(Arc::new(Self {
            locked: Mutex::new(Locked {
                store,
                limiter: RateLimiter::new(config.send_per_second, config.send_per_minute),
                reading_limiter: RateLimiter::new(5, 60),
                uploads: Uploads::default(),
            }),
            hub: hub::Hub::default(),
            verifier: cindy::Verifier::new(config.issuers.clone()),
        }))
    }

    /// 在存储锁里跑一段同步代码（放在阻塞线程池里，不占异步线程）。
    pub(crate) async fn with_store<T: Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut Locked, &hub::Hub) -> T + Send + 'static,
    ) -> T {
        let app = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut locked = app
                .locked
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            f(&mut locked, &app.hub)
        })
        .await
        .expect("存储任务不会 panic")
    }
}

/// 路由。
pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/ws", get(ws::upgrade))
        .route(
            "/v1/files/{file}",
            get(files::download)
                .head(files::progress)
                .put(files::upload),
        )
        .layer(DefaultBodyLimit::max(
            friend_im_core::limits::UPLOAD_PART_MAX,
        ))
        .with_state(app)
}

/// 后台定时任务：作废过期的批准请求、清理没传完的文件。
pub fn spawn_maintenance(app: Arc<App>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(15));
        let mut rounds: u64 = 0;
        loop {
            tick.tick().await;
            rounds += 1;
            let cleanup = rounds.is_multiple_of(240);
            app.with_store(move |locked, hub| {
                match locked.store.expire_friend_requests(now_ms()) {
                    Ok(effects) => {
                        hub.apply(&effects, None);
                    }
                    Err(error) => tracing::warn!(%error, "好友申请过期处理失败"),
                }
                match locked.store.expire_joins(now_ms()) {
                    Ok(effects) => {
                        hub.apply(&effects, None);
                    }
                    Err(error) => tracing::warn!(%error, "作废过期的批准请求失败"),
                }
                if cleanup {
                    match locked.store.cleanup_uploads(now_ms()) {
                        Ok(0) => {}
                        Ok(n) => tracing::info!(n, "清理了没传完的文件"),
                        Err(error) => tracing::warn!(%error, "清理没传完的文件失败"),
                    }
                }
            })
            .await;
        }
    });
}
