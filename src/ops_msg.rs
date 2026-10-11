//! 消息、历史、补拉与文件。

use std::collections::VecDeque;
use std::path::PathBuf;

use friend_im_core::bytes::Bytes;
use friend_im_core::file;
use friend_im_core::ids::{ConvId, FileId, MsgId, Uid};
use friend_im_core::limits;
use friend_im_core::message::{ENVELOPE_CT_MAX, Envelope};
use friend_im_core::wire::{ConvActivity, ErrorCode, Event, Push, Response, StoredMessage, SyncEvent, WireError};
use rusqlite::{OptionalExtension, params};

use crate::ops_account::{OpResult, Session, session_hash};
use crate::store::{DbResult, DeviceState, Effects, Store, Target, fail, from_json, internal, to_json};

/// 一次会话活动查询至多回几个会话（同 HHH 插件协议 `conversations/recentActivity` 的上限）。
const ACTIVITY_MAX: usize = 1000;

/// 发送频率记录：每个账号最近一分钟的发送时间。
pub struct RateLimiter {
    per_second: usize,
    per_minute: usize,
    sends: std::collections::HashMap<Uid, VecDeque<i64>>,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(limits::SEND_PER_SECOND, limits::SEND_PER_MINUTE)
    }
}

impl RateLimiter {
    /// 按给定上限新建。
    pub fn new(per_second: u32, per_minute: u32) -> Self {
        Self {
            per_second: per_second as usize,
            per_minute: per_minute as usize,
            sends: std::collections::HashMap::new(),
        }
    }

    /// 记一次发送；超过每秒或每分钟上限返回 `false`。
    pub fn allow(&mut self, uid: &Uid, now: i64) -> bool {
        let (per_second, per_minute) = (self.per_second, self.per_minute);
        let times = self.sends.entry(uid.clone()).or_default();
        while times.front().is_some_and(|t| *t <= now - 60_000) {
            times.pop_front();
        }
        let last_second = times.iter().filter(|t| **t > now - 1_000).count();
        if last_second >= per_second || times.len() >= per_minute {
            return false;
        }
        times.push_back(now);
        // 很久没发的账号不留记录，内存不随账号数无限增长。
        if self.sends.len() > 10_000 {
            self.sends.retain(|_, t| t.back().is_some_and(|last| *last > now - 60_000));
        }
        true
    }
}

/// 上传中的文件，防止同一个文件被两个请求同时写。
#[derive(Default)]
pub struct Uploads {
    active: std::collections::HashSet<FileId>,
}

/// 一次分段上传：写到哪个文件、从哪里开始。
pub struct UploadSlot {
    pub file: FileId,
    pub path: PathBuf,
    pub offset: u64,
    pub size: u64,
}

/// 库里一个文件：（上传者，会话，密文大小，已收到，建文件时的日志序号，是否传完）。
type FileRow = (Uid, ConvId, u64, u64, i64, bool);

impl Store {
    /// 发消息。
    pub fn send(&mut self, s: &Session, env: &Envelope, limiter: &mut RateLimiter, now: i64) -> OpResult {
        self.require_active(s)?;
        if env.sender != s.uid || env.device != s.device {
            return Err(fail(ErrorCode::Forbidden, "只能用自己这台电脑发消息"));
        }
        if env.ct.len() > ENVELOPE_CT_MAX || env.ct.is_empty() {
            return Err(fail(ErrorCode::LimitExceeded, "消息太大"));
        }
        let existing: Option<(i64, i64, String)> = self
            .conn
            .query_row(
                "SELECT log, ts_ms, sender FROM messages WHERE conv = ?1 AND msg = ?2",
                params![env.conv.as_str(), env.id.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .db()?;
        if let Some((log, ts_ms, sender)) = existing {
            // 同一条重发（拿不准服务器收没收到时）：返回同一个序号，不会出现两条。
            if sender == s.uid.as_str() {
                return Ok((Response::Sent { log: log as u64, ts_ms }, Effects::default()));
            }
            return Err(fail(ErrorCode::Conflict, "消息编号已被占用"));
        }
        if self.membership(&env.conv, &s.uid)? != Some(None) {
            return Err(fail(ErrorCode::Forbidden, "不是这个会话现在的成员"));
        }
        let eldest = self.current_state(&s.uid)?.map(|st| st.eldest);
        if eldest != Some(env.sender_eldest) {
            return Err(fail(ErrorCode::Invalid, "发送的身份指纹不是当前这一轮"));
        }
        let state = self.conv_state(&env.conv)?.ok_or_else(|| fail(ErrorCode::NotFound, "没有这个会话"))?;
        let rotation_needed: i64 = self
            .conn
            .query_row("SELECT rotation_needed FROM convs WHERE conv = ?1", params![env.conv.as_str()], |r| r.get(0))
            .db()?;
        if rotation_needed != 0 || state.rotation_pending {
            return Err(fail(ErrorCode::RotationRequired, "这个会话要先换代"));
        }
        if env.generation != state.current_key().generation {
            // 发送方看到的会话签名链落后了（别人刚换过代）：刷新后用新一代重新加密。
            return Err(fail(ErrorCode::Conflict, "会话钥匙已换代，请刷新后重发"));
        }
        if !limiter.allow(&s.uid, now) {
            return Err(fail(ErrorCode::RateLimited, "发送太频繁，请稍后再试"));
        }
        if let Some(target) = &env.recall {
            let found: Option<(String, i64)> = self
                .conn
                .query_row(
                    "SELECT sender, recalled FROM messages WHERE conv = ?1 AND msg = ?2",
                    params![env.conv.as_str(), target.as_str()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .db()?;
            match found {
                Some((sender, _)) if sender != s.uid.as_str() => return Err(fail(ErrorCode::Forbidden, "只能撤回自己的消息")),
                None => return Err(fail(ErrorCode::NotFound, "要撤回的消息不存在")),
                _ => {}
            }
        }
        let members = self.current_members(&env.conv)?;
        let tx = self.conn.unchecked_transaction().db()?;
        let log = Self::append_log(&tx, now, "message", None)?;
        tx.execute(
            "INSERT INTO messages(log, conv, msg, sender, ts_ms, env) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params![log as i64, env.conv.as_str(), env.id.as_str(), s.uid.as_str(), now, to_json(env)?],
        )
        .db()?;
        if let Some(target) = &env.recall {
            self.tombstone(&tx, &env.conv, target)?;
        }
        Self::fan_out(&tx, log, &members)?;
        tx.execute("UPDATE convs SET last_log = ?1 WHERE conv = ?2", params![log as i64, env.conv.as_str()]).db()?;
        tx.commit().db()?;
        self.last_log = log;
        let event = SyncEvent {
            log,
            ts_ms: now,
            event: Event::Message {
                msg: StoredMessage {
                    log,
                    ts_ms: now,
                    env: env.clone(),
                    recalled: false,
                },
            },
        };
        let mut effects = Effects::default();
        for uid in members {
            effects.push(Target::Account(uid), Push::Event { event: event.clone() });
        }
        Ok((Response::Sent { log, ts_ms: now }, effects))
    }

    /// 撤回：把原消息的密文删掉，只留消息头（谁、什么时候），服务器上不再有这条的内容。
    fn tombstone(&self, conn: &rusqlite::Connection, conv: &ConvId, target: &MsgId) -> Result<(), WireError> {
        let env_text: String = conn
            .query_row("SELECT env FROM messages WHERE conv = ?1 AND msg = ?2", params![conv.as_str(), target.as_str()], |r| r.get(0))
            .db()?;
        let mut env: Envelope = from_json(&env_text)?;
        env.ct = Bytes::default();
        conn.execute(
            "UPDATE messages SET recalled = 1, env = ?1 WHERE conv = ?2 AND msg = ?3",
            params![to_json(&env)?, conv.as_str(), target.as_str()],
        )
        .db()?;
        Ok(())
    }

    fn stored(log: i64, ts_ms: i64, env: &str, recalled: i64) -> Result<StoredMessage, WireError> {
        Ok(StoredMessage {
            log: log as u64,
            ts_ms,
            env: from_json(env)?,
            recalled: recalled != 0,
        })
    }

    /// 往前翻历史（离开过的人只能看到离开为止）。`since_ms` / `until_ms` 只要这段服务器时间里的（两端都含）。
    pub fn history(&mut self, s: &Session, conv: &ConvId, before: Option<u64>, limit: u32, since_ms: Option<i64>, until_ms: Option<i64>) -> OpResult {
        self.require_active(s)?;
        let cap = self.membership(conv, &s.uid)?.ok_or_else(|| fail(ErrorCode::Forbidden, "不是这个会话的成员"))?;
        let limit = limit.clamp(1, limits::HISTORY_PAGE_MAX);
        let before = before.map(|b| b as i64).unwrap_or(i64::MAX);
        let cap = cap.map(|c| c as i64).unwrap_or(i64::MAX);
        let Some((lo, hi)) = self.log_window(conv, since_ms, until_ms)? else {
            return Ok((Response::Messages { items: Vec::new(), more: false }, Effects::default()));
        };
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT log, ts_ms, env, recalled FROM messages WHERE conv = ?1 AND log < ?2 AND log <= ?3 AND log >= ?5 AND log <= ?6 \
                 AND ts_ms >= ?7 AND ts_ms <= ?8 ORDER BY log DESC LIMIT ?4",
            )
            .db()?;
        let (since, until) = (since_ms.unwrap_or(i64::MIN), until_ms.unwrap_or(i64::MAX));
        let rows = stmt
            .query_map(params![conv.as_str(), before, cap, limit as i64 + 1, lo, hi, since, until], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?, r.get::<_, i64>(3)?))
            })
            .db()?;
        let mut items = Vec::new();
        let mut bytes = 0;
        let mut more = false;
        for row in rows {
            let (log, ts_ms, env, recalled) = row.db()?;
            // 条数或字节数到了就停（至少给一条），一页回执不会大到拖垮两边。
            if items.len() == limit as usize || (!items.is_empty() && bytes + env.len() > limits::RESPONSE_SOFT_MAX_BYTES) {
                more = true;
                break;
            }
            bytes += env.len();
            items.push(Self::stored(log, ts_ms, &env, recalled)?);
        }
        Ok((Response::Messages { items, more }, Effects::default()))
    }

    /// 时间窗口换成序号范围（含两端），窗口里没有消息时为 `None`。服务器按收到的先后给序号、记时间，序号越大时间越晚，
    /// 所以按“会话 + 时间”的索引各取一端就够：翻到很早的历史也不用从最新一条往下扫。主查询仍带着时间条件，时钟回拨时也不会多给。
    fn log_window(&self, conv: &ConvId, since_ms: Option<i64>, until_ms: Option<i64>) -> Result<Option<(i64, i64)>, WireError> {
        let lo = match since_ms {
            None => 0,
            Some(since) => {
                let mut stmt = self
                    .conn
                    .prepare_cached("SELECT log FROM messages WHERE conv = ?1 AND ts_ms >= ?2 ORDER BY ts_ms, log LIMIT 1")
                    .db()?;
                match stmt.query_row(params![conv.as_str(), since], |r| r.get::<_, i64>(0)).optional().db()? {
                    Some(log) => log,
                    None => return Ok(None),
                }
            }
        };
        let hi = match until_ms {
            None => i64::MAX,
            Some(until) => {
                let mut stmt = self
                    .conn
                    .prepare_cached("SELECT log FROM messages WHERE conv = ?1 AND ts_ms <= ?2 ORDER BY ts_ms DESC, log DESC LIMIT 1")
                    .db()?;
                match stmt.query_row(params![conv.as_str(), until], |r| r.get::<_, i64>(0)).optional().db()? {
                    Some(log) => log,
                    None => return Ok(None),
                }
            }
        };
        Ok((lo <= hi).then_some((lo, hi)))
    }

    /// 会话活动：自己所在（或离开前所在）的会话里 `since_ms` 起有消息的，每个会话给最早、最新一条的服务器时间，
    /// 按最新一条从新到旧，至多 [`ACTIVITY_MAX`] 个。只回编号和时间，不碰内容；离开过的会话只算离开之前的。
    pub fn activity(&mut self, s: &Session, since_ms: i64) -> OpResult {
        self.require_active(s)?;
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT m.conv, MIN(m.ts_ms), MAX(m.ts_ms) FROM conv_members c JOIN messages m ON m.conv = c.conv \
                 WHERE c.uid = ?1 AND m.ts_ms >= ?2 AND (c.left_log IS NULL OR m.log <= c.left_log) \
                 GROUP BY m.conv ORDER BY MAX(m.ts_ms) DESC LIMIT ?3",
            )
            .db()?;
        let rows = stmt
            .query_map(params![s.uid.as_str(), since_ms, ACTIVITY_MAX as i64], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?))
            })
            .db()?;
        let mut items = Vec::new();
        for row in rows {
            let (conv, first_ms, latest_ms) = row.db()?;
            items.push(ConvActivity { conv: ConvId::parse(&conv).map_err(internal)?, first_ms, latest_ms });
        }
        Ok((Response::Activity { items }, Effects::default()))
    }

    /// 补拉序号大于 `since` 的事件。
    pub fn sync(&mut self, s: &Session, since: u64, limit: u32) -> OpResult {
        self.require_active(s)?;
        let limit = limit.clamp(1, limits::SYNC_PAGE_MAX);
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT f.seq, l.ts_ms, l.kind, l.payload, m.env, m.recalled FROM feed f JOIN log l ON l.seq = f.seq \
                 LEFT JOIN messages m ON m.log = f.seq WHERE f.uid = ?1 AND f.seq > ?2 ORDER BY f.seq LIMIT ?3",
            )
            .db()?;
        let rows = stmt
            .query_map(params![s.uid.as_str(), since as i64, limit as i64 + 1], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, Option<i64>>(5)?,
                ))
            })
            .db()?;
        let mut events = Vec::new();
        let mut bytes = 0;
        let mut more = false;
        for row in rows {
            let (seq, ts_ms, kind, payload, env, recalled) = row.db()?;
            let size = payload.as_ref().map_or(0, String::len) + env.as_ref().map_or(0, String::len);
            if events.len() == limit as usize || (!events.is_empty() && bytes + size > limits::RESPONSE_SOFT_MAX_BYTES) {
                more = true;
                break;
            }
            bytes += size;
            let event = match (kind.as_str(), payload, env) {
                ("message", _, Some(env)) => Event::Message {
                    msg: Self::stored(seq, ts_ms, &env, recalled.unwrap_or(0))?,
                },
                (_, Some(payload), _) => from_json(&payload)?,
                _ => return Err(internal("日志缺内容")),
            };
            events.push(SyncEvent {
                log: seq as u64,
                ts_ms,
                event,
            });
        }
        let next = events.last().map(|e| e.log).unwrap_or(since);
        Ok((Response::SyncBatch { events, next, more }, Effects::default()))
    }

    /// 新建一个文件。
    pub fn file_create(&mut self, s: &Session, conv: &ConvId, size: u64, now: i64) -> OpResult {
        self.require_active(s)?;
        if self.membership(conv, &s.uid)? != Some(None) {
            return Err(fail(ErrorCode::Forbidden, "不是这个会话现在的成员"));
        }
        if file::plain_len(size).is_none() || size > limits::file_sealed_max() {
            return Err(fail(ErrorCode::LimitExceeded, "文件大小不对或超过 100 MiB"));
        }
        let account = self.account(&s.uid)?.ok_or_else(|| internal("找不到账号"))?;
        if account.storage_bytes + size > limits::ACCOUNT_STORAGE_BYTES {
            return Err(fail(ErrorCode::LimitExceeded, "这个账号存的文件已到上限"));
        }
        let id = FileId::random();
        let tx = self.conn.unchecked_transaction().db()?;
        tx.execute(
            "INSERT INTO files(file, uid, conv, size, created_ms, created_log) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params![id.as_str(), s.uid.as_str(), conv.as_str(), size as i64, now, self.last_log as i64],
        )
        .db()?;
        tx.execute("UPDATE accounts SET storage_bytes = storage_bytes + ?1 WHERE uid = ?2", params![size as i64, s.uid.as_str()])
            .db()?;
        tx.commit().db()?;
        Ok((Response::FileCreated { file: id }, Effects::default()))
    }

    /// HTTP 请求的登录凭证 → 已批准的电脑。
    pub fn http_session(&self, token: &[u8]) -> Result<Session, WireError> {
        let found: Option<(String, String, String)> = self
            .conn
            .query_row("SELECT device, uid, status FROM devices WHERE session_hash = ?1", params![session_hash(token).to_vec()], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .optional()
            .db()?;
        let (device, uid, status) = found.ok_or_else(|| fail(ErrorCode::Unauthorized, "登录凭证无效"))?;
        if DeviceState::parse(&status) != DeviceState::Active {
            return Err(fail(ErrorCode::NotApproved, "这台电脑还没被批准"));
        }
        Ok(Session {
            uid: Uid::parse(&uid).map_err(internal)?,
            device: friend_im_core::ids::DeviceId::parse(&device).map_err(internal)?,
        })
    }

    fn file_row(&self, file: &FileId) -> Result<Option<FileRow>, WireError> {
        let row: Option<(String, String, i64, i64, i64, i64)> = self
            .conn
            .query_row(
                "SELECT uid, conv, size, received, created_log, complete FROM files WHERE file = ?1",
                params![file.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
            )
            .optional()
            .db()?;
        row.map(|(uid, conv, size, received, created_log, complete)| {
            Ok((
                Uid::parse(&uid).map_err(internal)?,
                ConvId::parse(&conv).map_err(internal)?,
                size as u64,
                received as u64,
                created_log,
                complete != 0,
            ))
        })
        .transpose()
    }

    /// 查上传进度（断点续传）：返回（已收到，总大小）。
    pub fn file_progress(&self, s: &Session, file: &FileId) -> Result<(u64, u64), WireError> {
        let (owner, _, size, received, _, _) = self.file_row(file)?.ok_or_else(|| fail(ErrorCode::NotFound, "没有这个文件"))?;
        if owner != s.uid {
            return Err(fail(ErrorCode::Forbidden, "不是你上传的文件"));
        }
        Ok((received, size))
    }

    /// 开始写一段：位置必须正好接着已收到的部分。
    pub fn upload_begin(&self, uploads: &mut Uploads, s: &Session, file: &FileId, offset: u64, len: u64) -> Result<UploadSlot, WireError> {
        let (owner, _, size, received, _, complete) = self.file_row(file)?.ok_or_else(|| fail(ErrorCode::NotFound, "没有这个文件"))?;
        if owner != s.uid {
            return Err(fail(ErrorCode::Forbidden, "不是你上传的文件"));
        }
        if complete || offset != received || offset + len > size || len == 0 || len > limits::UPLOAD_PART_MAX as u64 {
            return Err(fail(ErrorCode::Conflict, format!("位置不对：已收到 {received} 字节")));
        }
        if !uploads.active.insert(file.clone()) {
            return Err(fail(ErrorCode::Conflict, "这个文件正在上传"));
        }
        Ok(UploadSlot {
            file: file.clone(),
            path: self.file_path(file.as_str()),
            offset,
            size,
        })
    }

    /// 一段写完（`written` 为实际写入的字节数，失败时为 0）。
    pub fn upload_end(&self, uploads: &mut Uploads, slot: &UploadSlot, written: u64) -> Result<u64, WireError> {
        uploads.active.remove(&slot.file);
        let received = slot.offset + written;
        self.conn
            .execute(
                "UPDATE files SET received = ?1, complete = ?2 WHERE file = ?3",
                params![received as i64, i64::from(received == slot.size), slot.file.as_str()],
            )
            .db()?;
        Ok(received)
    }

    /// 下载：完整的文件，会话成员（离开过的人只能下离开前的）。
    pub fn download(&self, s: &Session, file: &FileId) -> Result<(PathBuf, u64), WireError> {
        let (_, conv, size, _, created_log, complete) = self.file_row(file)?.ok_or_else(|| fail(ErrorCode::NotFound, "没有这个文件"))?;
        match self.membership(&conv, &s.uid)? {
            Some(None) => {}
            Some(Some(left)) if created_log as u64 <= left => {}
            _ => return Err(fail(ErrorCode::Forbidden, "不是这个会话的成员")),
        }
        if !complete {
            return Err(fail(ErrorCode::NotFound, "文件还没传完"));
        }
        Ok((self.file_path(file.as_str()), size))
    }

    /// 定时任务：一天还没传完的文件删掉，释放额度。
    pub fn cleanup_uploads(&mut self, now: i64) -> Result<usize, WireError> {
        let stale: Vec<(String, String, i64)> = {
            let mut stmt = self.conn.prepare_cached("SELECT file, uid, size FROM files WHERE complete = 0 AND created_ms < ?1").db()?;
            let rows = stmt.query_map(params![now - 86_400_000], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).db()?;
            rows.collect::<Result<_, _>>().db()?
        };
        for (file, uid, size) in &stale {
            let _ = std::fs::remove_file(self.file_path(file));
            let tx = self.conn.unchecked_transaction().db()?;
            tx.execute("DELETE FROM files WHERE file = ?1", params![file]).db()?;
            tx.execute("UPDATE accounts SET storage_bytes = MAX(0, storage_bytes - ?1) WHERE uid = ?2", params![size, uid])
                .db()?;
            tx.commit().db()?;
        }
        Ok(stale.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiter_enforces_per_second_and_per_minute() {
        let mut limiter = RateLimiter::default();
        let uid = Uid::random();
        for i in 0..5 {
            assert!(limiter.allow(&uid, 1_000 + i));
        }
        assert!(!limiter.allow(&uid, 1_005));
        // 过了一秒又能发，但一分钟内总数不超过 120。
        let mut sent = 5;
        let mut t = 2_000;
        while limiter.allow(&uid, t) {
            sent += 1;
            t += 250;
        }
        assert_eq!(sent, 120);
        assert!(limiter.allow(&uid, 1_000 + 61_000));
    }
}
