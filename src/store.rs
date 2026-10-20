//! 存储：一个 SQLite 文件（WAL 模式）+ 一个文件目录。服务器的全部读写只走这一层，
//! 以后要换 PostgreSQL 只改这里。
//!
//! - 所有写入在一个数据库事务里完成：批准新电脑、移除电脑、重置、移人 + 换代这类操作要么整批生效，要么不生效。
//! - 每条消息、会话签名链变化、钥匙封装、账号签名链变化都占一个全局递增的日志序号（`log` 表），
//!   再给每个收件账号写一行 `feed`（写时扇出）：补拉就是按账号查 `feed`，权限天然隔离。
//! - 签名链在内存里缓存验过的现状（[`AccountState`]、[`ConvState`]），缓存只在事务提交后更新。
//!
//! 库里只有密文、公钥、签名和编号；不存 Cindy 凭证，登录凭证只存摘要。

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use friend_im_core::account::{AccountLink, AccountState};
use friend_im_core::bytes::Fixed;
use friend_im_core::conv::{ConvLink, ConvState, Directory};
use friend_im_core::ids::{ConvId, DeviceId, Uid};
use friend_im_core::keys::DevicePublic;
use friend_im_core::wire::{DeviceKind, ErrorCode, Push, WireError};
use rusqlite::{Connection, OptionalExtension, params};

/// 库结构版本：改表结构时加 1 并写迁移。
const SCHEMA_VERSION: i64 = 2;
/// 签名链现状缓存的条数上限：超过就整体清空重新按需加载（1000 个账号的规模下很少触发）。
const CACHE_MAX: usize = 20_000;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS accounts(
    uid TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    epoch INTEGER NOT NULL DEFAULT 0,
    provisioned INTEGER NOT NULL DEFAULT 0,
    rotation_needed INTEGER NOT NULL DEFAULT 0,
    friend_token BLOB UNIQUE,
    storage_bytes INTEGER NOT NULL DEFAULT 0,
    created_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS devices(
    device TEXT PRIMARY KEY,
    uid TEXT NOT NULL REFERENCES accounts(uid),
    sign_pub BLOB NOT NULL,
    enc_pub BLOB NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'unknown',
    status TEXT NOT NULL,
    session_hash BLOB UNIQUE,
    created_ms INTEGER NOT NULL,
    joined_ms INTEGER NOT NULL DEFAULT 0,
    last_seen_ms INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS devices_uid ON devices(uid, status);
CREATE TABLE IF NOT EXISTS account_links(
    uid TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    link TEXT NOT NULL,
    log INTEGER NOT NULL,
    PRIMARY KEY(uid, epoch, seq)
);
CREATE TABLE IF NOT EXISTS account_keys(
    uid TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    device TEXT NOT NULL,
    sealed TEXT NOT NULL,
    PRIMARY KEY(uid, epoch, generation, device)
);
CREATE TABLE IF NOT EXISTS account_prev(
    uid TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    wrapped BLOB NOT NULL,
    PRIMARY KEY(uid, epoch, generation)
);
CREATE TABLE IF NOT EXISTS joins(
    request TEXT PRIMARY KEY,
    uid TEXT NOT NULL,
    device TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    expires_ms INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    pake BLOB NOT NULL,
    proof BLOB NOT NULL,
    responder TEXT,
    answer TEXT,
    open INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS joins_uid ON joins(uid, created_ms);
CREATE TABLE IF NOT EXISTS convs(
    conv TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    dm_key TEXT UNIQUE,
    created_ms INTEGER NOT NULL,
    rotation_needed INTEGER NOT NULL DEFAULT 0,
    last_log INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS conv_links(
    conv TEXT NOT NULL,
    seq INTEGER NOT NULL,
    link TEXT NOT NULL,
    log INTEGER NOT NULL,
    PRIMARY KEY(conv, seq)
);
CREATE TABLE IF NOT EXISTS conv_members(
    conv TEXT NOT NULL,
    uid TEXT NOT NULL,
    joined_log INTEGER NOT NULL,
    left_log INTEGER,
    PRIMARY KEY(conv, uid, joined_log)
);
CREATE INDEX IF NOT EXISTS conv_members_uid ON conv_members(uid, conv);
CREATE TABLE IF NOT EXISTS conv_keys(
    conv TEXT NOT NULL,
    generation INTEGER NOT NULL,
    uid TEXT NOT NULL,
    account_epoch INTEGER NOT NULL,
    account_generation INTEGER NOT NULL,
    sealed TEXT NOT NULL,
    PRIMARY KEY(conv, generation, uid)
);
CREATE TABLE IF NOT EXISTS conv_prev(
    conv TEXT NOT NULL,
    generation INTEGER NOT NULL,
    wrapped BLOB NOT NULL,
    PRIMARY KEY(conv, generation)
);
CREATE TABLE IF NOT EXISTS invites(
    invite TEXT PRIMARY KEY,
    conv TEXT NOT NULL,
    generation INTEGER NOT NULL,
    seed BLOB NOT NULL,
    expires_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages(
    log INTEGER PRIMARY KEY,
    conv TEXT NOT NULL,
    msg TEXT NOT NULL,
    sender TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    env TEXT NOT NULL,
    recalled INTEGER NOT NULL DEFAULT 0,
    UNIQUE(conv, msg)
);
CREATE INDEX IF NOT EXISTS messages_conv ON messages(conv, log);
-- 按时间读历史、会话活动：时间窗口先在这里换成序号范围（v0.2.0 加，老库打开时补建，表结构不变）。
CREATE INDEX IF NOT EXISTS messages_conv_ts ON messages(conv, ts_ms, log);
CREATE TABLE IF NOT EXISTS files(
    file TEXT PRIMARY KEY,
    uid TEXT NOT NULL,
    conv TEXT NOT NULL,
    size INTEGER NOT NULL,
    received INTEGER NOT NULL DEFAULT 0,
    created_ms INTEGER NOT NULL,
    created_log INTEGER NOT NULL,
    complete INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS log(
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT
);
CREATE TABLE IF NOT EXISTS feed(
    uid TEXT NOT NULL,
    seq INTEGER NOT NULL,
    PRIMARY KEY(uid, seq)
) WITHOUT ROWID;
"#;

/// 一次操作要发出的推送：事务提交后、仍持有存储锁时按顺序发出，保证每台电脑收到的推送和日志顺序一致。
#[derive(Default, Debug)]
pub struct Effects {
    /// 推送。
    pub pushes: Vec<(Target, Push)>,
    /// 要断开的电脑（被移除、重置后作废）。
    pub close: Vec<DeviceId>,
    /// 刚被批准、之后可以收会话推送的电脑。
    pub approve: Vec<DeviceId>,
}

/// 推送的目标。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    /// 某账号所有已批准的电脑。
    Account(Uid),
    /// 某账号所有已批准的电脑，跳过已经由局域网提交的电脑。
    AccountExcept(Uid, Vec<DeviceId>),
    /// 某台电脑（含还没批准的）。
    Device(DeviceId),
}

impl Effects {
    pub fn push(&mut self, target: Target, push: Push) {
        self.pushes.push((target, push));
    }
}

/// 电脑在库里的状态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeviceState {
    /// 还没批准（或账号还没开通）。
    Pending,
    /// 已批准。
    Active,
    /// 已移除。
    Removed,
}

impl DeviceState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Active => "active",
            Self::Removed => "removed",
        }
    }

    pub fn parse(text: &str) -> Self {
        match text {
            "active" => Self::Active,
            "removed" => Self::Removed,
            _ => Self::Pending,
        }
    }
}

/// 库里的一台电脑。
#[derive(Clone, Debug)]
pub struct DeviceRow {
    pub device: DeviceId,
    pub uid: Uid,
    pub keys: DevicePublic,
    pub name: String,
    pub kind: DeviceKind,
    pub state: DeviceState,
    pub joined_ms: i64,
    pub last_seen_ms: i64,
}

/// 库里的一个账号。
#[derive(Clone, Debug)]
pub struct AccountRow {
    pub uid: Uid,
    pub name: String,
    pub epoch: u32,
    pub provisioned: bool,
    pub rotation_needed: bool,
    pub storage_bytes: u64,
}

/// 内部错误：数据库出错时记日志，对外只报“服务器内部错误”。
pub fn internal(error: impl std::fmt::Display) -> WireError {
    tracing::error!(%error, "存储出错");
    WireError::new(ErrorCode::Internal, "服务器内部错误")
}

/// 构造对外错误。
pub fn fail(code: ErrorCode, message: impl Into<String>) -> WireError {
    WireError::new(code, message)
}

pub(crate) trait DbResult<T> {
    fn db(self) -> Result<T, WireError>;
}

impl<T> DbResult<T> for rusqlite::Result<T> {
    fn db(self) -> Result<T, WireError> {
        self.map_err(internal)
    }
}

pub(crate) fn to_json<T: serde::Serialize>(value: &T) -> Result<String, WireError> {
    serde_json::to_string(value).map_err(internal)
}

pub(crate) fn from_json<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, WireError> {
    serde_json::from_str(text).map_err(internal)
}

/// 某账号各轮签名链的现状缓存。
#[derive(Default, Clone)]
pub(crate) struct AccountCache {
    pub epochs: BTreeMap<u32, AccountState>,
}

/// 存储。
pub struct Store {
    pub(crate) conn: Connection,
    pub(crate) accounts: HashMap<Uid, AccountCache>,
    pub(crate) convs: HashMap<ConvId, ConvState>,
    pub(crate) files_dir: PathBuf,
    /// 最近一个已提交的日志序号。
    pub(crate) last_log: u64,
}

impl Store {
    /// 打开（或新建）数据目录里的库。
    pub fn open(data_dir: &Path) -> Result<Self, String> {
        std::fs::create_dir_all(data_dir).map_err(|e| format!("建不了数据目录：{e}"))?;
        let files_dir = data_dir.join("files");
        std::fs::create_dir_all(&files_dir).map_err(|e| format!("建不了文件目录：{e}"))?;
        let conn = Connection::open(data_dir.join("friend-im.sqlite3")).map_err(|e| format!("打不开数据库：{e}"))?;
        Self::init(conn, files_dir)
    }

    fn init(conn: Connection, files_dir: PathBuf) -> Result<Self, String> {
        conn.pragma_update(None, "journal_mode", "WAL").map_err(|e| e.to_string())?;
        conn.pragma_update(None, "synchronous", "NORMAL").map_err(|e| e.to_string())?;
        conn.pragma_update(None, "foreign_keys", "ON").map_err(|e| e.to_string())?;
        conn.busy_timeout(std::time::Duration::from_secs(5)).map_err(|e| e.to_string())?;
        conn.execute_batch(SCHEMA).map_err(|e| format!("建表失败：{e}"))?;
        let version: Option<String> = conn
            .query_row("SELECT value FROM meta WHERE key = 'schema_version'", [], |r| r.get(0))
            .optional()
            .map_err(|e| e.to_string())?;
        match version.as_deref().map(str::parse::<i64>) {
            None => {
                conn.execute("INSERT INTO meta(key, value) VALUES('schema_version', ?1)", params![SCHEMA_VERSION.to_string()])
                    .map_err(|e| e.to_string())?;
            }
            Some(Ok(v)) if v == SCHEMA_VERSION => {}
            Some(Ok(1)) => {
                conn.execute("ALTER TABLE devices ADD COLUMN kind TEXT NOT NULL DEFAULT 'computer'", []).map_err(|e| e.to_string())?;
                conn.execute("UPDATE meta SET value = ?1 WHERE key = 'schema_version'", params![SCHEMA_VERSION.to_string()]).map_err(|e| e.to_string())?;
            }
            Some(other) => return Err(format!("数据库结构版本不认识：{other:?}")),
        }
        let last_log: i64 = conn.query_row("SELECT COALESCE(MAX(seq), 0) FROM log", [], |r| r.get(0)).map_err(|e| e.to_string())?;
        Ok(Self {
            conn,
            accounts: HashMap::new(),
            convs: HashMap::new(),
            files_dir,
            last_log: last_log as u64,
        })
    }

    /// 在线备份到另一个文件（SQLite backup 接口，不停服务）。
    pub fn backup_to(&self, path: &Path) -> Result<(), String> {
        let mut target = Connection::open(path).map_err(|e| e.to_string())?;
        let backup = rusqlite::backup::Backup::new(&self.conn, &mut target).map_err(|e| e.to_string())?;
        backup
            .run_to_completion(256, std::time::Duration::from_millis(5), None)
            .map_err(|e| e.to_string())
    }

    /// 文件在磁盘上的路径：按编号前两个字符分目录，单个目录不会堆太多文件。
    pub(crate) fn file_path(&self, file: &str) -> PathBuf {
        let prefix: String = file.chars().take(2).collect();
        self.files_dir.join(prefix).join(file)
    }

    pub(crate) fn trim_caches(&mut self) {
        if self.accounts.len() > CACHE_MAX {
            self.accounts.clear();
        }
        if self.convs.len() > CACHE_MAX {
            self.convs.clear();
        }
    }

    /// 写一条日志，返回日志序号（在事务里调用）。
    pub(crate) fn append_log(conn: &Connection, ts_ms: i64, kind: &str, payload: Option<&str>) -> Result<u64, WireError> {
        conn.execute("INSERT INTO log(ts_ms, kind, payload) VALUES(?1, ?2, ?3)", params![ts_ms, kind, payload])
            .db()?;
        Ok(conn.last_insert_rowid() as u64)
    }

    /// 给一批账号写同一条日志的 feed 行。
    pub(crate) fn fan_out<'a>(conn: &Connection, seq: u64, uids: impl IntoIterator<Item = &'a Uid>) -> Result<(), WireError> {
        let mut stmt = conn.prepare_cached("INSERT OR IGNORE INTO feed(uid, seq) VALUES(?1, ?2)").db()?;
        for uid in uids {
            stmt.execute(params![uid.as_str(), seq as i64]).db()?;
        }
        Ok(())
    }

    // ---------- 账号与电脑 ----------

    pub(crate) fn account(&self, uid: &Uid) -> Result<Option<AccountRow>, WireError> {
        self.conn
            .query_row(
                "SELECT name, epoch, provisioned, rotation_needed, storage_bytes FROM accounts WHERE uid = ?1",
                params![uid.as_str()],
                |r| {
                    Ok(AccountRow {
                        uid: uid.clone(),
                        name: r.get(0)?,
                        epoch: r.get::<_, i64>(1)? as u32,
                        provisioned: r.get::<_, i64>(2)? != 0,
                        rotation_needed: r.get::<_, i64>(3)? != 0,
                        storage_bytes: r.get::<_, i64>(4)? as u64,
                    })
                },
            )
            .optional()
            .db()
    }

    pub(crate) fn device(&self, device: &DeviceId) -> Result<Option<DeviceRow>, WireError> {
        self.conn
            .query_row(
                "SELECT uid, sign_pub, enc_pub, name, kind, status, joined_ms, last_seen_ms FROM devices WHERE device = ?1",
                params![device.as_str()],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, Vec<u8>>(2)?, r.get::<_, String>(3)?, r.get::<_, String>(4)?, r.get::<_, String>(5)?, r.get(6)?, r.get(7)?)),
            )
            .optional()
            .db()?
            .map(|(uid, sign, enc, name, kind, status, joined_ms, last_seen_ms)| {
                Ok(DeviceRow {
                    device: device.clone(),
                    uid: Uid::parse(&uid).map_err(internal)?,
                    keys: DevicePublic {
                        sign: Fixed::from_slice(&sign).ok_or_else(|| internal("签名公钥长度不对"))?,
                        enc: Fixed::from_slice(&enc).ok_or_else(|| internal("收件公钥长度不对"))?,
                    },
                    name,
                    kind: DeviceKind::parse(&kind),
                    state: DeviceState::parse(&status),
                    joined_ms,
                    last_seen_ms,
                })
            })
            .transpose()
    }

    pub(crate) fn devices_of(&self, uid: &Uid, state: DeviceState) -> Result<Vec<DeviceRow>, WireError> {
        let ids: Vec<String> = {
            let mut stmt = self.conn.prepare_cached("SELECT device FROM devices WHERE uid = ?1 AND status = ?2 ORDER BY joined_ms, created_ms").db()?;
            let rows = stmt.query_map(params![uid.as_str(), state.as_str()], |r| r.get(0)).db()?;
            rows.collect::<Result<_, _>>().db()?
        };
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(row) = self.device(&DeviceId::parse(&id).map_err(internal)?)? {
                out.push(row);
            }
        }
        Ok(out)
    }

    /// 某账号某一轮签名链的现状（按需从库里加载并重新验一遍）。
    pub(crate) fn account_state(&mut self, uid: &Uid, epoch: u32) -> Result<Option<AccountState>, WireError> {
        if let Some(state) = self.accounts.get(uid).and_then(|c| c.epochs.get(&epoch)) {
            return Ok(Some(state.clone()));
        }
        let links: Vec<(String, i64)> = {
            let mut stmt = self.conn.prepare_cached("SELECT link, log FROM account_links WHERE uid = ?1 AND epoch = ?2 ORDER BY seq").db()?;
            let rows = stmt.query_map(params![uid.as_str(), epoch as i64], |r| Ok((r.get(0)?, r.get(1)?))).db()?;
            rows.collect::<Result<_, _>>().db()?
        };
        if links.is_empty() {
            return Ok(None);
        }
        let parsed: Vec<(AccountLink, u64)> = links
            .iter()
            .map(|(text, log)| Ok((from_json::<AccountLink>(text)?, *log as u64)))
            .collect::<Result<_, WireError>>()?;
        let state = AccountState::verify_chain(uid, epoch, &parsed).map_err(|e| internal(format!("库里的账号签名链验不过：{e}")))?;
        self.trim_caches();
        self.accounts.entry(uid.clone()).or_default().epochs.insert(epoch, state.clone());
        Ok(Some(state))
    }

    /// 某账号当前一轮签名链的现状。
    pub(crate) fn current_state(&mut self, uid: &Uid) -> Result<Option<AccountState>, WireError> {
        let Some(account) = self.account(uid)? else {
            return Ok(None);
        };
        if !account.provisioned {
            return Ok(None);
        }
        self.account_state(uid, account.epoch)
    }

    /// 把一批账号的签名链（各轮）都装进缓存，之后可以用 [`CacheDirectory`] 同步地校验会话签名链。
    pub(crate) fn preload_accounts<'a>(&mut self, uids: impl IntoIterator<Item = &'a Uid>) -> Result<(), WireError> {
        for uid in uids {
            let epochs: Vec<i64> = {
                let mut stmt = self.conn.prepare_cached("SELECT DISTINCT epoch FROM account_links WHERE uid = ?1").db()?;
                let rows = stmt.query_map(params![uid.as_str()], |r| r.get(0)).db()?;
                rows.collect::<Result<_, _>>().db()?
            };
            for epoch in epochs {
                self.account_state(uid, epoch as u32)?;
            }
        }
        Ok(())
    }

    /// 会话签名链现状（按需从库里加载并重新验一遍）。
    pub(crate) fn conv_state(&mut self, conv: &ConvId) -> Result<Option<ConvState>, WireError> {
        if let Some(state) = self.convs.get(conv) {
            return Ok(Some(state.clone()));
        }
        let links: Vec<(String, i64)> = {
            let mut stmt = self.conn.prepare_cached("SELECT link, log FROM conv_links WHERE conv = ?1 ORDER BY seq").db()?;
            let rows = stmt.query_map(params![conv.as_str()], |r| Ok((r.get(0)?, r.get(1)?))).db()?;
            rows.collect::<Result<_, _>>().db()?
        };
        if links.is_empty() {
            return Ok(None);
        }
        let parsed: Vec<(ConvLink, u64)> = links
            .iter()
            .map(|(text, log)| Ok((from_json::<ConvLink>(text)?, *log as u64)))
            .collect::<Result<_, WireError>>()?;
        let authors = friend_im_core::conv::link_uids(parsed.iter().map(|(l, _)| l));
        self.preload_accounts(&authors)?;
        let directory = CacheDirectory { accounts: &self.accounts };
        let state = ConvState::verify_chain(conv, &parsed, &directory).map_err(|e| internal(format!("库里的会话签名链验不过：{e}")))?;
        self.trim_caches();
        self.convs.insert(conv.clone(), state.clone());
        Ok(Some(state))
    }

    /// 某账号所在会话的现在的成员（含自己），去重。
    pub(crate) fn contacts(&self, uid: &Uid) -> Result<Vec<Uid>, WireError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT DISTINCT m2.uid FROM conv_members m1 JOIN conv_members m2 ON m1.conv = m2.conv \
                 WHERE m1.uid = ?1 AND m1.left_log IS NULL AND m2.left_log IS NULL",
            )
            .db()?;
        let rows = stmt.query_map(params![uid.as_str()], |r| r.get::<_, String>(0)).db()?;
        let mut out: Vec<Uid> = Vec::new();
        for row in rows {
            out.push(Uid::parse(&row.db()?).map_err(internal)?);
        }
        if !out.contains(uid) {
            out.push(uid.clone());
        }
        Ok(out)
    }

    /// 某账号在某会话里的成员区间：现在是成员返回 `Some(None)`，离开过返回 `Some(Some(离开时的日志序号))`，从没进过返回 `None`。
    pub(crate) fn membership(&self, conv: &ConvId, uid: &Uid) -> Result<Option<Option<u64>>, WireError> {
        let rows: Vec<Option<i64>> = {
            let mut stmt = self.conn.prepare_cached("SELECT left_log FROM conv_members WHERE conv = ?1 AND uid = ?2").db()?;
            let rows = stmt.query_map(params![conv.as_str(), uid.as_str()], |r| r.get(0)).db()?;
            rows.collect::<Result<_, _>>().db()?
        };
        if rows.is_empty() {
            return Ok(None);
        }
        if rows.iter().any(Option::is_none) {
            return Ok(Some(None));
        }
        Ok(Some(rows.into_iter().flatten().max().map(|l| l as u64)))
    }

    /// 某会话现在的成员（按库里的成员区间，服务器用它决定推给谁）。
    pub(crate) fn current_members(&self, conv: &ConvId) -> Result<Vec<Uid>, WireError> {
        let mut stmt = self.conn.prepare_cached("SELECT uid FROM conv_members WHERE conv = ?1 AND left_log IS NULL").db()?;
        let rows = stmt.query_map(params![conv.as_str()], |r| r.get::<_, String>(0)).db()?;
        let mut out = Vec::new();
        for row in rows {
            out.push(Uid::parse(&row.db()?).map_err(internal)?);
        }
        Ok(out)
    }
}

/// 用内存里验过的账号签名链回答“这台电脑当时有没有效”。
pub(crate) struct CacheDirectory<'a> {
    pub accounts: &'a HashMap<Uid, AccountCache>,
}

impl Directory for CacheDirectory<'_> {
    fn device(&self, uid: &Uid, eldest: &Fixed<32>, device: &DeviceId, log: u64) -> Option<DevicePublic> {
        self.accounts
            .get(uid)?
            .epochs
            .values()
            .find(|s| &s.eldest == eldest)
            .and_then(|s| s.device_at(device, log))
    }

    fn eldest(&self, uid: &Uid) -> Option<Fixed<32>> {
        self.accounts.get(uid)?.epochs.values().next_back().map(|s| s.eldest)
    }
}

#[cfg(test)]
pub(crate) fn open_memory() -> Store {
    let dir = std::env::temp_dir().join(format!("friend-im-test-{}", friend_im_core::ids::ConvId::random()));
    std::fs::create_dir_all(&dir).unwrap();
    Store::init(Connection::open_in_memory().unwrap(), dir).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cindy::CindyIdentity;
    use crate::ops_account::LoginAuth;
    use friend_im_core::keys::DeviceSecret;

    #[test]
    fn schema_is_created_and_reopen_keeps_version() {
        let dir = std::env::temp_dir().join(format!("friend-im-schema-{}", ConvId::random()));
        let store = Store::open(&dir).unwrap();
        assert_eq!(store.last_log, 0);
        drop(store);
        let store = Store::open(&dir).unwrap();
        let version: String = store.conn.query_row("SELECT value FROM meta WHERE key='schema_version'", [], |r| r.get(0)).unwrap();
        assert_eq!(version, "2");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn v1_schema_migrates_device_kind_as_computer() {
        let dir = std::env::temp_dir().join(format!("friend-im-schema-v1-{}", ConvId::random()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("friend-im.sqlite3");
        let conn = Connection::open(&db).unwrap();
        let uid = Uid::from_cindy("https://auth.cindy.app", "migration");
        let device = DeviceSecret::generate();
        conn.execute_batch(
            "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES ('schema_version', '1');
             CREATE TABLE accounts(uid TEXT PRIMARY KEY, name TEXT NOT NULL, epoch INTEGER NOT NULL DEFAULT 0, provisioned INTEGER NOT NULL DEFAULT 0, rotation_needed INTEGER NOT NULL DEFAULT 0, friend_token BLOB UNIQUE, storage_bytes INTEGER NOT NULL DEFAULT 0, created_ms INTEGER NOT NULL);
             CREATE TABLE devices(device TEXT PRIMARY KEY, uid TEXT NOT NULL, sign_pub BLOB NOT NULL, enc_pub BLOB NOT NULL, name TEXT NOT NULL, status TEXT NOT NULL, session_hash BLOB UNIQUE, created_ms INTEGER NOT NULL, joined_ms INTEGER NOT NULL DEFAULT 0, last_seen_ms INTEGER NOT NULL DEFAULT 0);
             CREATE TABLE log(seq INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, kind TEXT NOT NULL, payload TEXT);",
        )
        .unwrap();
        conn.execute("INSERT INTO accounts(uid, name, created_ms) VALUES(?1, 'migration', 1)", params![uid.as_str()]).unwrap();
        conn.execute("INSERT INTO devices(device, uid, sign_pub, enc_pub, name, status, created_ms) VALUES(?1, ?2, ?3, ?4, '旧电脑', 'pending', 1)", params![device.id().as_str(), uid.as_str(), device.public().sign.0.to_vec(), device.public().enc.0.to_vec()]).unwrap();
        drop(conn);

        let store = Store::open(&dir).unwrap();
        let row = store.device(&device.id()).unwrap().unwrap();
        assert_eq!(row.kind, DeviceKind::Computer);
        let version: String = store.conn.query_row("SELECT value FROM meta WHERE key='schema_version'", [], |r| r.get(0)).unwrap();
        assert_eq!(version, "2");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn login_persists_mobile_device_kind() {
        let mut store = open_memory();
        let identity = CindyIdentity { issuer: "https://auth.cindy.app".into(), member: "mobile-user".into(), name: "手机用户".into() };
        let device = DeviceSecret::generate();
        let session = store
            .login(LoginAuth::Cindy(identity), device.public(), "我的手机", DeviceKind::Mobile, 1)
            .unwrap()
            .0;
        assert_eq!(store.device(&session.device).unwrap().unwrap().kind, DeviceKind::Mobile);
    }

    #[test]
    fn log_sequence_is_monotonic() {
        let store = open_memory();
        let a = Store::append_log(&store.conn, 1, "x", None).unwrap();
        let b = Store::append_log(&store.conn, 1, "x", None).unwrap();
        assert!(b > a);
    }
}
