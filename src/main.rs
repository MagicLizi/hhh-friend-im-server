//! Friend-IM 服务器入口。
//!
//! - `friend-im-server`：按环境变量启动（见 `.env.example`）。
//! - `friend-im-server backup <目标文件>`：在线备份数据库（SQLite backup 接口，不停服务也能做）。
//! - `friend-im-server healthcheck`：问本机的 `/healthz`，给容器健康检查用（运行镜像里不装 curl）。

use friend_im_server::config::Config;
use friend_im_server::{App, router, spawn_maintenance};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        // 接终端时才上色：容器日志、日志文件里不要颜色码。
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout()))
        .init();
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(%error, "配置不对");
            std::process::exit(2);
        }
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("healthcheck") {
        std::process::exit(if healthy(config.listen) { 0 } else { 1 });
    }
    if args.first().map(String::as_str) == Some("backup") {
        let Some(target) = args.get(1) else {
            eprintln!("用法：friend-im-server backup <目标文件>");
            std::process::exit(2);
        };
        let store = match friend_im_server::store::Store::open(&config.data_dir) {
            Ok(store) => store,
            Err(error) => {
                tracing::error!(%error, "打不开数据库");
                std::process::exit(1);
            }
        };
        match store.backup_to(std::path::Path::new(target)) {
            Ok(()) => tracing::info!(target, "备份完成"),
            Err(error) => {
                tracing::error!(%error, "备份失败");
                std::process::exit(1);
            }
        }
        return;
    }
    let app = match App::new(&config) {
        Ok(app) => app,
        Err(error) => {
            tracing::error!(%error, "启动失败");
            std::process::exit(1);
        }
    };
    spawn_maintenance(app.clone());
    let listener = match tokio::net::TcpListener::bind(config.listen).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(%error, listen = %config.listen, "监听失败");
            std::process::exit(1);
        }
    };
    tracing::info!(listen = %config.listen, data = %config.data_dir.display(), "Friend-IM 服务器已启动");
    let shutdown = async {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("能注册 SIGTERM");
            tokio::select! {
                _ = ctrl_c => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = ctrl_c.await;
        }
        tracing::info!("收到停止信号，正在退出");
    };
    if let Err(error) = axum::serve(listener, router(app)).with_graceful_shutdown(shutdown).await {
        tracing::error!(%error, "服务器出错退出");
        std::process::exit(1);
    }
}

/// 连本机监听的端口问一次 `/healthz`，3 秒内回 200 才算健康。
fn healthy(listen: std::net::SocketAddr) -> bool {
    use std::io::{Read, Write};
    let port = listen.port();
    let Ok(mut stream) = std::net::TcpStream::connect_timeout(&([127, 0, 0, 1], port).into(), std::time::Duration::from_secs(3)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(3)));
    if stream.write_all(b"GET /healthz HTTP/1.0\r\nHost: localhost\r\n\r\n").is_err() {
        return false;
    }
    let mut head = [0u8; 12];
    stream.read_exact(&mut head).is_ok() && head.starts_with(b"HTTP/1.") && &head[9..12] == b"200"
}
