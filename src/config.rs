//! 配置：全部来自环境变量，仓库里只有示例（`.env.example`），不放任何真实部署配置。

use std::net::SocketAddr;
use std::path::PathBuf;

use friend_im_core::limits;

/// 一个 Cindy 签发方：签发方地址 + 公钥地址。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Issuer {
    /// JWT 里的 `iss`。
    pub issuer: String,
    /// JWKS 地址。
    pub jwks_url: String,
}

/// 服务器配置。
#[derive(Clone, Debug)]
pub struct Config {
    /// 监听地址（容器里 `0.0.0.0:8080`，宿主机只把它绑到 127.0.0.1，前面一层反向代理挂证书）。
    pub listen: SocketAddr,
    /// 数据目录：SQLite 文件与文件目录都在这里。
    pub data_dir: PathBuf,
    /// 认可的 Cindy 签发方。
    pub issuers: Vec<Issuer>,
    /// 每个账号每秒最多发几条（默认见 `friend_im_core::limits`）。
    pub send_per_second: u32,
    /// 每个账号每分钟最多发几条。
    pub send_per_minute: u32,
}

/// Cindy 的两个签发方（与 HHH `hhh-friend/src/auth/types.rs` 一致）。
pub fn default_issuers() -> Vec<Issuer> {
    ["https://auth.cindy.app", "https://auth.cindy.com.cn"]
        .into_iter()
        .map(|issuer| Issuer {
            issuer: issuer.into(),
            jwks_url: format!("{issuer}/.well-known/jwks.json"),
        })
        .collect()
}

/// 解析 `FRIEND_IM_CINDY_ISSUERS`：`签发方=公钥地址;签发方=公钥地址`（只在测试或私有部署时设置）。
pub fn parse_issuers(text: &str) -> Result<Vec<Issuer>, String> {
    let mut out = Vec::new();
    for part in text.split(';').map(str::trim).filter(|p| !p.is_empty()) {
        let (issuer, jwks_url) = part.split_once('=').ok_or_else(|| format!("签发方配置应是 签发方=公钥地址：{part}"))?;
        out.push(Issuer {
            issuer: issuer.trim().into(),
            jwks_url: jwks_url.trim().into(),
        });
    }
    if out.is_empty() {
        return Err("签发方配置是空的".into());
    }
    Ok(out)
}

impl Config {
    /// 从环境变量读配置。
    pub fn from_env() -> Result<Self, String> {
        let listen = std::env::var("FRIEND_IM_LISTEN")
            .unwrap_or_else(|_| "0.0.0.0:8080".into())
            .parse()
            .map_err(|_| "FRIEND_IM_LISTEN 不是合法的地址".to_string())?;
        let data_dir = std::env::var("FRIEND_IM_DATA_DIR").unwrap_or_else(|_| "/data".into()).into();
        let issuers = match std::env::var("FRIEND_IM_CINDY_ISSUERS") {
            Ok(text) => parse_issuers(&text)?,
            Err(_) => default_issuers(),
        };
        Ok(Self {
            listen,
            data_dir,
            issuers,
            send_per_second: env_u32("FRIEND_IM_SEND_PER_SECOND", limits::SEND_PER_SECOND)?,
            send_per_minute: env_u32("FRIEND_IM_SEND_PER_MINUTE", limits::SEND_PER_MINUTE)?,
        })
    }

    /// 测试与私有部署用：给定数据目录与签发方，其余取默认值。
    pub fn new(data_dir: PathBuf, issuers: Vec<Issuer>) -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            data_dir,
            issuers,
            send_per_second: limits::SEND_PER_SECOND,
            send_per_minute: limits::SEND_PER_MINUTE,
        }
    }
}

fn env_u32(name: &str, default: u32) -> Result<u32, String> {
    match std::env::var(name) {
        Ok(text) => text.trim().parse().ok().filter(|v| *v > 0).ok_or_else(|| format!("{name} 应是正整数")),
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issuers_parse_and_reject_garbage() {
        let parsed = parse_issuers("https://a=http://127.0.0.1:9/jwks ; https://b=http://x/j").unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].issuer, "https://a");
        assert_eq!(parsed[1].jwks_url, "http://x/j");
        assert!(parse_issuers("").is_err());
        assert!(parse_issuers("no-equals").is_err());
        assert_eq!(default_issuers()[0].jwks_url, "https://auth.cindy.app/.well-known/jwks.json");
    }
}
