//! 核对 Cindy 身份凭证：只用 Cindy 公开的公钥验签，不需要 Cindy 配合。
//!
//! 规则与 HHH `hhh-friend/src/auth/verifier.rs` 同一套：RS256；两个签发方；公钥缓存 5 分钟，
//! 遇到不认识的 `kid` 最快 30 秒强刷一次；`aud` 为 `cindy`、`typ` 为 `access`；签发与过期时间容差 5 分钟；
//! 有效期不超过 1 小时；令牌 32 KiB 上限。
//!
//! 凭证只在每台电脑第一次连接时出现一次，核对完就丢掉：不存、不进日志。

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::jwk::{AlgorithmParameters, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::config::Issuer;

/// 官方 JWKS 的 max-age 是五分钟。
const JWKS_TTL: Duration = Duration::from_secs(300);
/// 不认识的 `kid` 最快三十秒强刷一次，防止被拿来刷 Cindy 的公钥接口。
const JWKS_REFRESH_COOLDOWN: Duration = Duration::from_secs(30);
/// 与 Cindy 服务器的时间允许相差五分钟（与 HHH 一致）。
const CLOCK_TOLERANCE_SECS: u64 = 300;
/// access token 最长一小时。
const ACCESS_MAX_TTL_SECS: u64 = 3600;
/// 令牌长度上限。
const TOKEN_MAX_BYTES: usize = 32 * 1024;
/// 一个签发方的公钥最多几把。
const JWKS_MAX_KEYS: usize = 16;
/// 取公钥的超时。
const JWKS_TIMEOUT: Duration = Duration::from_secs(10);
/// 名字最长多少个字符（Cindy 的名字是用户自己填的，截断防止超长数据）。
const NAME_MAX_CHARS: usize = 64;

/// 核对通过的 Cindy 身份。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CindyIdentity {
    /// 签发方。
    pub issuer: String,
    /// membership 编号（个人身份、每个组织身份各一个）。
    pub member: String,
    /// 名字。
    pub name: String,
}

/// 核对失败的原因（只给日志和错误码用，不带令牌内容）。
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CindyError {
    /// 令牌无效。
    #[error("Cindy 凭证无效")]
    Invalid,
    /// 令牌时间与服务器相差超过容差（多半是已过期）。
    #[error("Cindy 凭证已过期或时间不对")]
    Expired,
    /// 一时取不到 Cindy 的公钥。
    #[error("暂时取不到 Cindy 的公钥")]
    KeysUnavailable,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Claims {
    sub: String,
    typ: String,
    #[serde(deserialize_with = "numeric_date")]
    iat: u64,
    #[serde(deserialize_with = "numeric_date")]
    exp: u64,
    ctx: String,
    #[serde(default)]
    org: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
}

/// JS 里最大的安全整数：时间超过它的不收。
const SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;

/// JWT 的时间（RFC 7519 NumericDate）可以带小数秒，Cindy 自 2026-10-16 起签发时间就带：取整秒（向下取整，与 HHH 一致）。
/// 负数、非有限值与超出安全整数的不收。
fn numeric_date<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let number = serde_json::Number::deserialize(deserializer)?;
    if let Some(seconds) = number.as_u64() {
        return Ok(seconds);
    }
    number
        .as_f64()
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0 && *seconds <= SAFE_INTEGER_MAX as f64)
        .map(|seconds| seconds.floor() as u64)
        .ok_or_else(|| serde::de::Error::custom("时间不是非负的秒数"))
}

#[derive(Default)]
struct KeyCache {
    keys: Option<(Instant, JwkSet)>,
    last_attempt: Option<Instant>,
}

/// 核对器：每个签发方一份公钥缓存。
pub struct Verifier {
    issuers: Vec<Issuer>,
    caches: HashMap<String, Mutex<KeyCache>>,
    http: reqwest::Client,
}

/// 与 HHH `validate_membership_id` 相同：1–128 个字母数字、`-`、`_`，排除 Windows 保留名。
fn valid_member(value: &str) -> bool {
    let upper = value.to_ascii_uppercase();
    let reserved = matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (upper.len() == 4 && (upper.starts_with("COM") || upper.starts_with("LPT")) && matches!(upper.as_bytes()[3], b'1'..=b'9'));
    !value.is_empty() && value.len() <= 128 && !reserved && value.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// 不验签先读出 `iss`，只用来挑签发方；真正的核对在后面验签时完成。
fn peek_issuer(token: &str) -> Option<String> {
    let payload = token.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value.get("iss")?.as_str().map(str::to_owned)
}

impl Verifier {
    /// 新建。
    pub fn new(issuers: Vec<Issuer>) -> Self {
        let caches = issuers.iter().map(|i| (i.issuer.clone(), Mutex::new(KeyCache::default()))).collect();
        let http = reqwest::Client::builder().timeout(JWKS_TIMEOUT).build().expect("HTTP 客户端配置固定，不会失败");
        Self { issuers, caches, http }
    }

    /// 核对一张 Cindy 凭证。
    pub async fn verify(&self, token: &str) -> Result<CindyIdentity, CindyError> {
        if token.is_empty() || token.len() > TOKEN_MAX_BYTES {
            return Err(CindyError::Invalid);
        }
        let header = decode_header(token).map_err(|_| CindyError::Invalid)?;
        if header.alg != Algorithm::RS256 {
            return Err(CindyError::Invalid);
        }
        let kid = header.kid.filter(|v| !v.is_empty() && v.len() <= 128).ok_or(CindyError::Invalid)?;
        let issuer_name = peek_issuer(token).ok_or(CindyError::Invalid)?;
        let issuer = self.issuers.iter().find(|i| i.issuer == issuer_name).ok_or(CindyError::Invalid)?;
        let key = self.key(issuer, &kid).await?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[issuer.issuer.as_str()]);
        validation.set_audience(&["cindy"]);
        validation.set_required_spec_claims(&["iss", "aud", "sub", "exp", "iat"]);
        // 过期与签发时间放到最后按容差判断，才能把“时间不对”和“无效”分开报。
        validation.validate_exp = false;
        let claims = decode::<Claims>(token, &key, &validation).map_err(|_| CindyError::Invalid)?.claims;
        let now = unix_now();
        if claims.typ != "access"
            || !valid_member(&claims.sub)
            || claims.exp <= claims.iat
            || claims.exp - claims.iat > ACCESS_MAX_TTL_SECS
            || !matches!(claims.ctx.as_str(), "personal" | "org")
            || (claims.ctx == "org" && claims.org.as_deref().is_none_or(str::is_empty))
        {
            return Err(CindyError::Invalid);
        }
        if claims.iat > now.saturating_add(CLOCK_TOLERANCE_SECS) || claims.exp.saturating_add(CLOCK_TOLERANCE_SECS) < now {
            return Err(CindyError::Expired);
        }
        let name: String = claims
            .display_name
            .filter(|n| !n.trim().is_empty())
            .unwrap_or_else(|| "Cindy 用户".into())
            .chars()
            .filter(|c| !c.is_control())
            .take(NAME_MAX_CHARS)
            .collect();
        Ok(CindyIdentity {
            issuer: issuer.issuer.clone(),
            member: claims.sub,
            name,
        })
    }

    async fn key(&self, issuer: &Issuer, kid: &str) -> Result<DecodingKey, CindyError> {
        let mut cache = self.caches.get(&issuer.issuer).ok_or(CindyError::Invalid)?.lock().await;
        let expired = cache.keys.as_ref().is_none_or(|(at, _)| at.elapsed() >= JWKS_TTL);
        let unknown = cache.keys.as_ref().is_none_or(|(_, set)| set.find(kid).is_none());
        if expired || unknown {
            let cooling = cache.last_attempt.is_some_and(|at| at.elapsed() < JWKS_REFRESH_COOLDOWN);
            if !cooling {
                cache.last_attempt = Some(Instant::now());
                match self.fetch(issuer).await {
                    Ok(set) => cache.keys = Some((Instant::now(), set)),
                    Err(error) => tracing::warn!(issuer = %issuer.issuer, %error, "取 Cindy 公钥失败"),
                }
            }
        }
        let Some((_, set)) = cache.keys.as_ref() else {
            return Err(CindyError::KeysUnavailable);
        };
        let jwk = set.find(kid).ok_or(CindyError::Invalid)?;
        if !matches!(jwk.algorithm, AlgorithmParameters::RSA(_))
            || !matches!(jwk.common.key_algorithm, None | Some(KeyAlgorithm::RS256))
            || !matches!(jwk.common.public_key_use, None | Some(PublicKeyUse::Signature))
            || jwk.common.key_operations.as_ref().is_some_and(|ops| !ops.contains(&KeyOperations::Verify))
        {
            return Err(CindyError::Invalid);
        }
        DecodingKey::from_jwk(jwk).map_err(|_| CindyError::Invalid)
    }

    async fn fetch(&self, issuer: &Issuer) -> Result<JwkSet, String> {
        let response = self.http.get(&issuer.jwks_url).send().await.map_err(|e| format!("请求失败：{e}"))?;
        if !response.status().is_success() {
            return Err(format!("HTTP {}", response.status()));
        }
        let body = response.bytes().await.map_err(|e| format!("读取失败：{e}"))?;
        if body.len() > 64 * 1024 {
            return Err("公钥集合太大".into());
        }
        let set: JwkSet = serde_json::from_slice(&body).map_err(|_| "公钥集合读不懂".to_string())?;
        if set.keys.is_empty() || set.keys.len() > JWKS_MAX_KEYS {
            return Err("公钥数量不对".into());
        }
        Ok(set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_ids_follow_hhh_rules() {
        assert!(valid_member("mem_01H-x"));
        assert!(!valid_member(""));
        assert!(!valid_member("a/b"));
        assert!(!valid_member("COM1"));
        assert!(!valid_member(&"a".repeat(129)));
    }

    #[test]
    fn issuer_is_peeked_from_payload() {
        let payload = URL_SAFE_NO_PAD.encode(br#"{"iss":"https://auth.cindy.app"}"#);
        assert_eq!(peek_issuer(&format!("h.{payload}.s")).as_deref(), Some("https://auth.cindy.app"));
        assert_eq!(peek_issuer("garbage"), None);
    }

    // Cindy 自 2026-10-16 起签发时间带小数秒（RFC 7519 NumericDate 允许）：取整秒；负数、非数字、超大的不收。
    #[test]
    fn fractional_numeric_dates_are_read_as_whole_seconds() {
        let claims = |iat: serde_json::Value, exp: serde_json::Value| {
            serde_json::from_value::<Claims>(serde_json::json!({ "sub": "mem_1", "typ": "access", "iat": iat, "exp": exp, "ctx": "personal" }))
        };
        let read = claims(serde_json::json!(1_792_153_892.25), serde_json::json!(1_792_157_492.75)).unwrap();
        assert_eq!((read.iat, read.exp), (1_792_153_892, 1_792_157_492));
        let whole = claims(serde_json::json!(1_792_153_892u64), serde_json::json!(1_792_157_492u64)).unwrap();
        assert_eq!((whole.iat, whole.exp), (1_792_153_892, 1_792_157_492));
        for bad in [serde_json::json!(-1.5), serde_json::json!("1792153892"), serde_json::json!(f64::MAX)] {
            assert!(claims(bad.clone(), serde_json::json!(1_792_157_492u64)).is_err(), "{bad}");
        }
    }
}
