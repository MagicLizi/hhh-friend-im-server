//! 独立阅读密文；CAS 防止较早设备请求覆盖较新的记录，读取与重放都不改聊天事实。
use crate::{
    ops_account::{OpResult, Session},
    ops_msg::RateLimiter,
    store::{DbResult, Effects, Store, Target, fail, from_json, internal, to_json},
};
use friend_im_core::{
    ids::{ConvId, MsgId},
    reading::{ReadingEnvelope, StoredReading},
    wire::{ErrorCode, Push, Response, StoredMessage},
};
use rusqlite::{OptionalExtension, params};

impl Store {
    fn reading_scope(
        &self,
        s: &Session,
        conv: &ConvId,
    ) -> Result<(), friend_im_core::wire::WireError> {
        self.require_active(s)?;
        if self.membership(conv, &s.uid)? != Some(None) {
            return Err(fail(ErrorCode::Forbidden, "不是这个会话当前的成员"));
        }
        Ok(())
    }

    pub(crate) fn reading_messages(&self, s: &Session, conv: &ConvId, ids: &[MsgId]) -> OpResult {
        self.reading_scope(s, conv)?;
        if ids.len() > 100 {
            return Err(fail(ErrorCode::LimitExceeded, "一次最多核对 100 条消息"));
        }
        let mut statement = self
            .conn
            .prepare_cached(
                "SELECT log, ts_ms, env, recalled FROM messages WHERE conv=?1 AND msg=?2",
            )
            .db()?;
        let mut messages = Vec::new();
        for id in ids {
            let row: Option<(i64, i64, String, bool)> = statement
                .query_row(params![conv.as_str(), id.as_str()], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
                })
                .optional()
                .db()?;
            if let Some((log, ts_ms, json, recalled)) = row {
                messages.push(StoredMessage {
                    log: log as u64,
                    ts_ms,
                    env: from_json(&json)?,
                    recalled,
                });
            }
        }
        Ok((Response::ReadingMessages { messages }, Effects::default()))
    }

    pub(crate) fn reading_get(
        &self,
        s: &Session,
        conv: &ConvId,
        after: u64,
        limit: u32,
    ) -> OpResult {
        self.reading_scope(s, conv)?;
        if after > i64::MAX as u64 || limit == 0 || limit > 200 {
            return Err(fail(ErrorCode::BadRequest, "阅读分页边界无效"));
        }
        let mut statement = self.conn.prepare_cached("SELECT log,envelope FROM reading_states WHERE conv=?1 AND log>?2 ORDER BY log LIMIT ?3").db()?;
        let rows = statement
            .query_map(
                params![conv.as_str(), after as i64, i64::from(limit) + 1],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
            )
            .db()?;
        let mut records = rows
            .map(|row| {
                let (log, json) = row.db()?;
                Ok(StoredReading {
                    log: log as u64,
                    envelope: from_json(&json)?,
                })
            })
            .collect::<Result<Vec<_>, friend_im_core::wire::WireError>>()?;
        let more = records.len() > limit as usize;
        records.truncate(limit as usize);
        Ok((Response::ReadingPage { records, more }, Effects::default()))
    }

    pub(crate) fn reading_set(
        &mut self,
        s: &Session,
        envelope: &ReadingEnvelope,
        limiter: &mut RateLimiter,
        now: i64,
    ) -> OpResult {
        self.reading_scope(s, &envelope.conv)?;
        let device = self.require_active(s)?;
        if envelope.reader != s.uid || envelope.device != s.device {
            return Err(fail(ErrorCode::Forbidden, "只能写自己这台设备的阅读状态"));
        }
        envelope
            .verify(&device.keys)
            .map_err(|_| fail(ErrorCode::Invalid, "阅读签名无效"))?;
        let account = self
            .current_state(&s.uid)?
            .ok_or_else(|| fail(ErrorCode::NotFound, "没有这个账号"))?;
        let conv = self
            .conv_state(&envelope.conv)?
            .ok_or_else(|| fail(ErrorCode::NotFound, "没有这个会话"))?;
        if account.eldest != envelope.reader_eldest
            || conv.current_key().generation != envelope.generation
            || conv.rotation_pending
        {
            return Err(fail(
                ErrorCode::Conflict,
                "阅读身份或会话钥匙已变化，请先重新查询",
            ));
        }
        let previous: Option<(i64,i64,String)> = self.conn.query_row("SELECT revision,log,envelope FROM reading_states WHERE conv=?1 AND reader=?2 AND device=?3",params![envelope.conv.as_str(),s.uid.as_str(),s.device.as_str()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().db()?;
        let json = to_json(envelope)?;
        if let Some((revision, log, old)) = &previous
            && *revision as u64 == envelope.revision
            && old == &json
        {
            return Ok((
                Response::ReadingSaved { log: *log as u64 },
                Effects::default(),
            ));
        }
        let expected = previous
            .as_ref()
            .map(|v| v.0)
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| internal("阅读版本溢出"))?;
        if envelope.revision != expected as u64 {
            return Err(fail(ErrorCode::Conflict, "阅读记录已有新版，旧请求未写入"));
        }
        if !limiter.allow(&s.uid, now) {
            return Err(fail(ErrorCode::RateLimited, "阅读写回太频繁"));
        }
        let members = self.current_members(&envelope.conv)?;
        let tx = self.conn.unchecked_transaction().db()?;
        let log = Self::append_log(&tx, now, "reading", None)?;
        tx.execute("INSERT INTO reading_states(conv,reader,device,revision,log,envelope) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(conv,reader,device) DO UPDATE SET revision=excluded.revision,log=excluded.log,envelope=excluded.envelope",params![envelope.conv.as_str(),s.uid.as_str(),s.device.as_str(),envelope.revision as i64,log as i64,json]).db()?;
        tx.commit().db()?;
        self.last_log = log;
        let mut effects = Effects::default();
        for member in members {
            effects.push(
                Target::ReadingAccount(member),
                Push::ReadingChanged {
                    conv: envelope.conv.clone(),
                },
            );
        }
        Ok((Response::ReadingSaved { log }, effects))
    }
}
