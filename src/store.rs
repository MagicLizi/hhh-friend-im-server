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
const SCHEMA_VERSION: i64 = 4;
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

/// 清掉数据面：旧版的消息、文件、已读，以及日志里对应的消息事件。控制面的表一张不动。
const DROP_DATA_PLANE: &str = r#"
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS reading_states;
DROP TABLE IF EXISTS files;
DELETE FROM feed WHERE seq IN (SELECT seq FROM log WHERE kind = 'message');
DELETE FROM log WHERE kind = 'message';
UPDATE accounts SET storage_bytes = 0;
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
    /// 仅已协商好友申请扩展的连接。
    FriendAccount(Uid),
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
    /// 最近一个已提交的日志序号。
    pub(crate) last_log: u64,
}

impl Store {
    /// 打开（或新建）数据目录里的库。
    pub fn open(data_dir: &Path) -> Result<Self, String> {
        std::fs::create_dir_all(data_dir).map_err(|e| format!("建不了数据目录：{e}"))?;
        let conn = Connection::open(data_dir.join("friend-im.sqlite3"))
            .map_err(|e| format!("打不开数据库：{e}"))?;
        Self::init(conn, Some(data_dir.join("files")))
    }

    /// `files_dir` 是旧版存文件的目录：升级时删掉。
    fn init(conn: Connection, files_dir: Option<PathBuf>) -> Result<Self, String> {
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|e| e.to_string())?;
        // Check the version before schema writes, and migrate atomically. Version 3
        // intentionally refuses old servers: they do not enforce friend approval.
        // Version 4 refuses them too: they would start storing messages again.
        let has_meta: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta')",
                [],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        if has_meta {
            let current: Option<String> = conn
                .query_row(
                    "SELECT value FROM meta WHERE key='schema_version'",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| e.to_string())?;
            if !matches!(current.as_deref(), Some("1" | "2" | "3" | "4")) {
                return Err(format!("数据库结构版本不认识：{current:?}"));
            }
        }
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| e.to_string())?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(|e| e.to_string())?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(|e| e.to_string())?;
        let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
        tx.execute_batch(SCHEMA)
            .map_err(|e| format!("建表失败：{e}"))?;
        let version: Option<String> = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        let upgrading = matches!(version.as_deref(), Some("1" | "2" | "3"));
        match version.as_deref().map(str::parse::<i64>) {
            None => {
                tx.execute(
                    "INSERT INTO meta(key, value) VALUES('schema_version', ?1)",
                    params![SCHEMA_VERSION.to_string()],
                )
                .map_err(|e| e.to_string())?;
            }
            Some(Ok(v)) if v == SCHEMA_VERSION => {}
            Some(Ok(1)) => {
                tx.execute(
                    "ALTER TABLE devices ADD COLUMN kind TEXT NOT NULL DEFAULT 'computer'",
                    [],
                )
                .map_err(|e| e.to_string())?;
            }
            Some(Ok(2 | 3)) => {}
            Some(other) => return Err(format!("数据库结构版本不认识：{other:?}")),
        }
        tx.execute_batch(crate::ops_friends::SCHEMA)
            .map_err(|e| format!("好友申请迁移失败：{e}"))?;
        // 旧版存的消息、文件和已读：这一版开始服务器不存，升级时直接清掉（不搬，契约 friend-im v10 第 7 项）。
        if upgrading {
            tx.execute_batch(DROP_DATA_PLANE)
                .map_err(|e| format!("清旧消息失败：{e}"))?;
        }
        tx.commit().map_err(|e| e.to_string())?;
        // 库提交之后再删文件目录，删完才写新版本号：中断了，下次打开版本还是旧的，接着删。
        if upgrading
            && let Some(dir) = files_dir
        {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(format!("删旧文件目录失败：{error}")),
            }
        }
        conn.execute(
            "UPDATE meta SET value=?1 WHERE key='schema_version'",
            params![SCHEMA_VERSION.to_string()],
        )
        .map_err(|e| e.to_string())?;
        let last_log: i64 = conn
            .query_row("SELECT COALESCE(MAX(seq), 0) FROM log", [], |r| r.get(0))
            .map_err(|e| e.to_string())?;
        Ok(Self {
            conn,
            accounts: HashMap::new(),
            convs: HashMap::new(),
            last_log: last_log as u64,
        })
    }

    /// 在线备份到另一个文件（SQLite backup 接口，不停服务）。
    pub fn backup_to(&self, path: &Path) -> Result<(), String> {
        let mut target = Connection::open(path).map_err(|e| e.to_string())?;
        let backup =
            rusqlite::backup::Backup::new(&self.conn, &mut target).map_err(|e| e.to_string())?;
        backup
            .run_to_completion(256, std::time::Duration::from_millis(5), None)
            .map_err(|e| e.to_string())
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
    pub(crate) fn append_log(
        conn: &Connection,
        ts_ms: i64,
        kind: &str,
        payload: Option<&str>,
    ) -> Result<u64, WireError> {
        conn.execute(
            "INSERT INTO log(ts_ms, kind, payload) VALUES(?1, ?2, ?3)",
            params![ts_ms, kind, payload],
        )
        .db()?;
        Ok(conn.last_insert_rowid() as u64)
    }

    /// 给一批账号写同一条日志的 feed 行。
    pub(crate) fn fan_out<'a>(
        conn: &Connection,
        seq: u64,
        uids: impl IntoIterator<Item = &'a Uid>,
    ) -> Result<(), WireError> {
        let mut stmt = conn
            .prepare_cached("INSERT OR IGNORE INTO feed(uid, seq) VALUES(?1, ?2)")
            .db()?;
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

    pub(crate) fn devices_of(
        &self,
        uid: &Uid,
        state: DeviceState,
    ) -> Result<Vec<DeviceRow>, WireError> {
        let ids: Vec<String> = {
            let mut stmt = self.conn.prepare_cached("SELECT device FROM devices WHERE uid = ?1 AND status = ?2 ORDER BY joined_ms, created_ms").db()?;
            let rows = stmt
                .query_map(params![uid.as_str(), state.as_str()], |r| r.get(0))
                .db()?;
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
    pub(crate) fn account_state(
        &mut self,
        uid: &Uid,
        epoch: u32,
    ) -> Result<Option<AccountState>, WireError> {
        if let Some(state) = self.accounts.get(uid).and_then(|c| c.epochs.get(&epoch)) {
            return Ok(Some(state.clone()));
        }
        let links: Vec<(String, i64)> = {
            let mut stmt = self.conn.prepare_cached("SELECT link, log FROM account_links WHERE uid = ?1 AND epoch = ?2 ORDER BY seq").db()?;
            let rows = stmt
                .query_map(params![uid.as_str(), epoch as i64], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .db()?;
            rows.collect::<Result<_, _>>().db()?
        };
        if links.is_empty() {
            return Ok(None);
        }
        let parsed: Vec<(AccountLink, u64)> = links
            .iter()
            .map(|(text, log)| Ok((from_json::<AccountLink>(text)?, *log as u64)))
            .collect::<Result<_, WireError>>()?;
        let state = AccountState::verify_chain(uid, epoch, &parsed)
            .map_err(|e| internal(format!("库里的账号签名链验不过：{e}")))?;
        self.trim_caches();
        self.accounts
            .entry(uid.clone())
            .or_default()
            .epochs
            .insert(epoch, state.clone());
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
    pub(crate) fn preload_accounts<'a>(
        &mut self,
        uids: impl IntoIterator<Item = &'a Uid>,
    ) -> Result<(), WireError> {
        for uid in uids {
            let epochs: Vec<i64> = {
                let mut stmt = self
                    .conn
                    .prepare_cached("SELECT DISTINCT epoch FROM account_links WHERE uid = ?1")
                    .db()?;
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
            let mut stmt = self
                .conn
                .prepare_cached("SELECT link, log FROM conv_links WHERE conv = ?1 ORDER BY seq")
                .db()?;
            let rows = stmt
                .query_map(params![conv.as_str()], |r| Ok((r.get(0)?, r.get(1)?)))
                .db()?;
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
        let directory = CacheDirectory {
            accounts: &self.accounts,
        };
        let state = ConvState::verify_chain(conv, &parsed, &directory)
            .map_err(|e| internal(format!("库里的会话签名链验不过：{e}")))?;
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
        let rows = stmt
            .query_map(params![uid.as_str()], |r| r.get::<_, String>(0))
            .db()?;
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
    pub(crate) fn membership(
        &self,
        conv: &ConvId,
        uid: &Uid,
    ) -> Result<Option<Option<u64>>, WireError> {
        let rows: Vec<Option<i64>> = {
            let mut stmt = self
                .conn
                .prepare_cached("SELECT left_log FROM conv_members WHERE conv = ?1 AND uid = ?2")
                .db()?;
            let rows = stmt
                .query_map(params![conv.as_str(), uid.as_str()], |r| r.get(0))
                .db()?;
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
        let mut stmt = self
            .conn
            .prepare_cached("SELECT uid FROM conv_members WHERE conv = ?1 AND left_log IS NULL")
            .db()?;
        let rows = stmt
            .query_map(params![conv.as_str()], |r| r.get::<_, String>(0))
            .db()?;
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
    fn device(
        &self,
        uid: &Uid,
        eldest: &Fixed<32>,
        device: &DeviceId,
        log: u64,
    ) -> Option<DevicePublic> {
        self.accounts
            .get(uid)?
            .epochs
            .values()
            .find(|s| &s.eldest == eldest)
            .and_then(|s| s.device_at(device, log))
    }

    fn eldest(&self, uid: &Uid) -> Option<Fixed<32>> {
        self.accounts
            .get(uid)?
            .epochs
            .values()
            .next_back()
            .map(|s| s.eldest)
    }
}

#[cfg(test)]
pub(crate) fn open_memory() -> Store {
    Store::init(Connection::open_in_memory().unwrap(), None).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cindy::CindyIdentity;
    use crate::ops_account::LoginAuth;
    use friend_im_core::keys::DeviceSecret;

    #[test]
    fn unknown_schema_is_not_modified_and_v2_migration_is_backed_up() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("friend-im.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL); INSERT INTO meta VALUES('schema_version','99');").unwrap();
        drop(conn);
        let before = std::fs::read(&path).unwrap();
        assert!(Store::open(dir.path()).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("UPDATE meta SET value='2';").unwrap();
        drop(conn);
        let store = Store::open(dir.path()).unwrap();
        let backup = dir.path().join("backup.sqlite3");
        store.backup_to(&backup).unwrap();
        let restored = Connection::open(backup).unwrap();
        assert_eq!(
            restored
                .query_row(
                    "SELECT value FROM meta WHERE key='schema_version'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "4"
        );
        assert_eq!(
            restored
                .query_row("SELECT COUNT(*) FROM friend_requests", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    /// 建一个带旧数据的 schema 3 库：控制面的表由当前结构建好，再补上旧版存消息、文件、已读的表。
    fn old_data_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        drop(Store::open(dir.path()).unwrap());
        let conn = Connection::open(dir.path().join("friend-im.sqlite3")).unwrap();
        conn.execute_batch(
            "UPDATE meta SET value='3' WHERE key='schema_version';
             CREATE TABLE messages(log INTEGER PRIMARY KEY, conv TEXT NOT NULL, msg TEXT NOT NULL, sender TEXT NOT NULL, ts_ms INTEGER NOT NULL, env TEXT NOT NULL, recalled INTEGER NOT NULL DEFAULT 0);
             CREATE TABLE reading_states(conv TEXT NOT NULL, reader TEXT NOT NULL, device TEXT NOT NULL, revision INTEGER NOT NULL, log INTEGER NOT NULL, envelope TEXT NOT NULL);
             CREATE TABLE files(file TEXT PRIMARY KEY, uid TEXT NOT NULL, conv TEXT NOT NULL, size INTEGER NOT NULL, received INTEGER NOT NULL DEFAULT 0, created_ms INTEGER NOT NULL, created_log INTEGER NOT NULL, complete INTEGER NOT NULL DEFAULT 0);
             INSERT INTO accounts(uid, name, created_ms) VALUES('u1', '留下的账号', 1);
             INSERT INTO log(seq, ts_ms, kind, payload) VALUES(1, 1, 'conv_link', '{}'), (2, 2, 'message', NULL);
             INSERT INTO feed(uid, seq) VALUES('u1', 1), ('u1', 2);
             INSERT INTO messages(log, conv, msg, sender, ts_ms, env) VALUES(2, 'c', 'm', 'u1', 2, '旧信封');
             INSERT INTO files(file, uid, conv, size, created_ms, created_log) VALUES('f', 'u1', 'c', 1, 1, 1);
             INSERT INTO reading_states VALUES('c', 'u1', 'd', 1, 3, '旧已读');",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("files/ab")).unwrap();
        std::fs::write(dir.path().join("files/ab/f"), b"old").unwrap();
        dir
    }

    fn tables(path: &Path) -> Vec<String> {
        let conn = Connection::open(path).unwrap();
        let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'").unwrap();
        stmt.query_map([], |r| r.get(0)).unwrap().map(Result::unwrap).collect()
    }

    fn version(path: &Path) -> String {
        Connection::open(path).unwrap().query_row("SELECT value FROM meta WHERE key='schema_version'", [], |r| r.get(0)).unwrap()
    }

    #[test]
    fn upgrading_clears_old_messages_and_files_and_keeps_the_control_plane() {
        let dir = old_data_dir();
        let path = dir.path().join("friend-im.sqlite3");
        drop(Store::open(dir.path()).unwrap());
        assert_eq!(version(&path), "4");
        let names = tables(&path);
        for gone in ["messages", "files", "reading_states"] {
            assert!(!names.iter().any(|n| n == gone), "{gone} 应被清掉");
        }
        for kept in ["accounts", "devices", "convs", "conv_links", "conv_keys", "friend_requests"] {
            assert!(names.iter().any(|n| n == kept), "{kept} 要留着");
        }
        assert!(!dir.path().join("files").exists(), "文件目录清掉了");
        let conn = Connection::open(&path).unwrap();
        let count = |sql: &str| conn.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap();
        assert_eq!(count("SELECT COUNT(*) FROM accounts"), 1, "账号一个不少");
        assert_eq!(count("SELECT COUNT(*) FROM log WHERE kind='message'"), 0);
        assert_eq!(count("SELECT COUNT(*) FROM log"), 1, "控制面的日志留着");
        assert_eq!(count("SELECT COUNT(*) FROM feed"), 1);
    }

    #[test]
    fn an_upgrade_interrupted_before_the_files_are_gone_finishes_on_the_next_open() {
        let dir = old_data_dir();
        let path = dir.path().join("friend-im.sqlite3");
        // 库已经清完、提交了，文件目录删到一半断电：版本还是 3。
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(DROP_DATA_PLANE).unwrap();
        drop(conn);
        std::fs::remove_dir_all(dir.path().join("files/ab")).unwrap();
        std::fs::create_dir_all(dir.path().join("files/cd")).unwrap();
        std::fs::write(dir.path().join("files/cd/g"), b"old").unwrap();
        assert_eq!(version(&path), "3");

        drop(Store::open(dir.path()).unwrap());
        assert_eq!(version(&path), "4");
        assert!(!dir.path().join("files").exists(), "接着删完");
    }

    #[test]
    fn a_newer_schema_is_refused_and_reopening_the_new_one_does_nothing() {
        let dir = tempfile::tempdir().unwrap();
        drop(Store::open(dir.path()).unwrap());
        let path = dir.path().join("friend-im.sqlite3");
        // 新版本号之后不再碰数据目录。
        std::fs::create_dir_all(dir.path().join("files")).unwrap();
        drop(Store::open(dir.path()).unwrap());
        assert!(dir.path().join("files").exists());
        Connection::open(&path).unwrap().execute_batch("UPDATE meta SET value='5' WHERE key='schema_version'").unwrap();
        assert!(Store::open(dir.path()).is_err());
        // 旧程序只认 1–3：版本 4 它打不开（这里按旧程序的判断写死，防止以后悄悄改回去）。
        assert_eq!(SCHEMA_VERSION, 4);
    }

    #[test]
    fn schema_is_created_and_reopen_keeps_version() {
        let dir = std::env::temp_dir().join(format!("friend-im-schema-{}", ConvId::random()));
        let store = Store::open(&dir).unwrap();
        assert_eq!(store.last_log, 0);
        drop(store);
        let store = Store::open(&dir).unwrap();
        let version: String = store
            .conn
            .query_row(
                "SELECT value FROM meta WHERE key='schema_version'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(version, "4");
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
        conn.execute(
            "INSERT INTO accounts(uid, name, created_ms) VALUES(?1, 'migration', 1)",
            params![uid.as_str()],
        )
        .unwrap();
        conn.execute("INSERT INTO devices(device, uid, sign_pub, enc_pub, name, status, created_ms) VALUES(?1, ?2, ?3, ?4, '旧电脑', 'pending', 1)", params![device.id().as_str(), uid.as_str(), device.public().sign.0.to_vec(), device.public().enc.0.to_vec()]).unwrap();
        drop(conn);

        let store = Store::open(&dir).unwrap();
        let row = store.device(&device.id()).unwrap().unwrap();
        assert_eq!(row.kind, DeviceKind::Computer);
        let version: String = store
            .conn
            .query_row(
                "SELECT value FROM meta WHERE key='schema_version'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(version, "4");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn login_persists_mobile_device_kind() {
        let mut store = open_memory();
        let identity = CindyIdentity {
            issuer: "https://auth.cindy.app".into(),
            member: "mobile-user".into(),
            name: "手机用户".into(),
        };
        let device = DeviceSecret::generate();
        let session = store
            .login(
                LoginAuth::Cindy(identity),
                device.public(),
                "我的手机",
                DeviceKind::Mobile,
                1,
            )
            .unwrap()
            .0;
        assert_eq!(
            store.device(&session.device).unwrap().unwrap().kind,
            DeviceKind::Mobile
        );
    }

    #[test]
    fn log_sequence_is_monotonic() {
        let store = open_memory();
        let a = Store::append_log(&store.conn, 1, "x", None).unwrap();
        let b = Store::append_log(&store.conn, 1, "x", None).unwrap();
        assert!(b > a);
    }
}
