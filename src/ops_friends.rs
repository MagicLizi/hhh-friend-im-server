//! Account-scoped approval ledger. A pending request is never a conversation.
use crate::{
    ops_account::{OpResult, Session},
    store::{DbResult, Effects, Store, Target, fail, from_json, to_json},
};
use friend_im_core::{
    conv::{ConvChange, ConvKind, ConvLink},
    friends::{
        FriendRequestEnvelope, FriendRequestStatus as Status, FriendSource, REQUEST_COOLDOWN_MS,
        REQUEST_TTL_MS, StoredFriendRequest,
    },
    ids::{RequestId, Uid},
    wire::{ConvKeyBundle, ErrorCode, Push, Response, WireError},
};
use rusqlite::{OptionalExtension, params};

pub(crate) const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS friend_requests(
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    request TEXT NOT NULL UNIQUE,
    from_uid TEXT NOT NULL,
    to_uid TEXT NOT NULL,
    pair_key TEXT NOT NULL,
    status TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    expires_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL,
    record TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS friend_requests_from ON friend_requests(from_uid, seq DESC);
CREATE INDEX IF NOT EXISTS friend_requests_to ON friend_requests(to_uid, seq DESC);
CREATE INDEX IF NOT EXISTS friend_requests_pair ON friend_requests(pair_key, seq DESC);
CREATE INDEX IF NOT EXISTS friend_requests_expiry ON friend_requests(status, expires_ms);
CREATE UNIQUE INDEX IF NOT EXISTS friend_requests_one_pending ON friend_requests(pair_key) WHERE status='pending';
"#;

fn pair(a: &Uid, b: &Uid) -> String {
    if a < b {
        format!("{a}:{b}")
    } else {
        format!("{b}:{a}")
    }
}
fn state_name(s: Status) -> &'static str {
    match s {
        Status::Pending => "pending",
        Status::Accepted => "accepted",
        Status::Rejected => "rejected",
        Status::Withdrawn => "withdrawn",
        Status::Expired => "expired",
    }
}

pub(crate) fn changed(record: &StoredFriendRequest) -> Effects {
    let mut effects = Effects::default();
    for uid in [&record.envelope.body.from.uid, &record.envelope.body.to.uid] {
        effects.push(
            Target::FriendAccount(uid.clone()),
            Push::FriendRequestsChanged,
        );
    }
    effects
}

impl Store {
    pub(crate) fn friend_record(
        &self,
        s: &Session,
        id: &RequestId,
    ) -> Result<StoredFriendRequest, WireError> {
        self.require_active(s)?;
        let value: Option<String> = self.conn.query_row("SELECT record FROM friend_requests WHERE request=?1 AND (from_uid=?2 OR to_uid=?2)", params![id.as_str(),s.uid.as_str()],|r|r.get(0)).optional().db()?;
        value
            .map(|v| from_json(&v))
            .transpose()?
            .ok_or_else(|| fail(ErrorCode::NotFound, "没有这条好友申请"))
    }

    pub(crate) fn write_friend_record(
        conn: &rusqlite::Connection,
        record: &StoredFriendRequest,
    ) -> Result<(), WireError> {
        conn.execute(
            "UPDATE friend_requests SET status=?1,updated_ms=?2,record=?3 WHERE request=?4",
            params![
                state_name(record.status),
                record.updated_ms,
                to_json(record)?,
                record.envelope.body.id.as_str()
            ],
        )
        .db()?;
        Ok(())
    }

    fn check_friend_source(&mut self, envelope: &FriendRequestEnvelope) -> Result<(), WireError> {
        let b = &envelope.body;
        for member in [&b.from, &b.to] {
            if self
                .current_state(&member.uid)?
                .is_none_or(|a| a.eldest != member.eldest)
            {
                return Err(fail(ErrorCode::Conflict, "身份已变化，请重新发送好友申请"));
            }
        }
        match &b.source {
            FriendSource::Group { conv } => {
                let group = self
                    .conv_state(conv)?
                    .ok_or_else(|| fail(ErrorCode::NotFound, "共同群聊不存在"))?;
                if group.kind != ConvKind::Group
                    || [&b.from, &b.to].iter().any(|m| {
                        group
                            .members
                            .get(&m.uid)
                            .is_none_or(|v| v.eldest != m.eldest)
                    })
                {
                    return Err(fail(
                        ErrorCode::Forbidden,
                        "你或对方已不在共同群聊，申请已失效",
                    ));
                }
            }
            FriendSource::Link { token } => {
                let valid: bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM accounts WHERE uid=?1 AND friend_token=?2 AND provisioned=1)",params![b.to.uid.as_str(),token.0.as_slice()],|r|r.get(0)).db()?;
                if !valid {
                    return Err(fail(ErrorCode::Forbidden, "好友链接已失效，请重新获取"));
                }
            }
        }
        Ok(())
    }

    fn refresh_friend_record(
        &mut self,
        record: &mut StoredFriendRequest,
        now: i64,
    ) -> Result<bool, WireError> {
        if record.status != Status::Pending {
            return Ok(false);
        }
        let invalid = match self.check_friend_source(&record.envelope) {
            Ok(()) => false,
            Err(error)
                if matches!(
                    error.code,
                    ErrorCode::Conflict | ErrorCode::Forbidden | ErrorCode::NotFound
                ) =>
            {
                true
            }
            Err(error) => return Err(error),
        };
        if record.expires_ms <= now || invalid {
            record.status = Status::Expired;
            record.updated_ms = now;
            Self::write_friend_record(&self.conn, record)?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn friend_request_send(
        &mut self,
        s: &Session,
        envelope: &FriendRequestEnvelope,
        now: i64,
    ) -> OpResult {
        self.require_active(s)?;
        let b = &envelope.body;
        if b.from.uid != s.uid || envelope.device != s.device {
            return Err(fail(ErrorCode::Forbidden, "只能发送自己的好友申请"));
        }
        let device = self
            .device(&s.device)?
            .ok_or_else(|| fail(ErrorCode::Forbidden, "设备无效"))?;
        envelope
            .verify(&device.keys)
            .map_err(|e| fail(ErrorCode::Invalid, e.to_string()))?;
        match self.friend_record(s, &b.id) {
            Ok(mut existing) => {
                if existing.envelope != *envelope {
                    return Err(fail(ErrorCode::Conflict, "申请编号已被使用"));
                }
                let effects = if self.refresh_friend_record(&mut existing, now)? {
                    changed(&existing)
                } else {
                    Effects::default()
                };
                return Ok((Response::FriendRequest { request: Box::new(existing) }, effects));
            }
            Err(error) if error.code == ErrorCode::NotFound => {}
            Err(error) => return Err(error),
        }
        self.check_friend_source(envelope)?;
        for m in &b.messages {
            let a = self
                .current_state(&m.uid)?
                .ok_or_else(|| fail(ErrorCode::Invalid, "账号未开通"))?;
            if a.epoch != m.epoch || a.current_key().generation != m.generation {
                return Err(fail(ErrorCode::RotationRequired, "账号钥匙已变化，请重试"));
            }
        }
        let key = pair(&b.from.uid, &b.to.uid);
        let exists: bool = self
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM convs WHERE dm_key=?1)",
                params![key],
                |r| r.get(0),
            )
            .db()?;
        if exists {
            return Err(fail(ErrorCode::Conflict, "已经是好友，请刷新名片"));
        }
        let mut effects = self.expire_friend_requests(now)?;
        let latest: Option<String> = self
            .conn
            .query_row(
                "SELECT record FROM friend_requests WHERE pair_key=?1 ORDER BY seq DESC LIMIT 1",
                params![key],
                |r| r.get(0),
            )
            .optional()
            .db()?;
        if let Some(json) = latest {
            let mut old: StoredFriendRequest = from_json(&json)?;
            if self.refresh_friend_record(&mut old, now)? {
                effects.pushes.extend(changed(&old).pushes);
            }
            if old.status == Status::Pending {
                return Ok((Response::FriendRequest { request: Box::new(old) }, effects));
            }
            if matches!(old.status, Status::Rejected | Status::Withdrawn)
                && now < old.updated_ms.saturating_add(REQUEST_COOLDOWN_MS)
            {
                return Err(fail(
                    ErrorCode::RateLimited,
                    "请在上次拒绝或撤回 24 小时后重新申请",
                ));
            }
        }
        let counts:(i64,i64,i64)=self.conn.query_row("SELECT COUNT(*),COALESCE(SUM(CASE WHEN created_ms>?2 AND from_uid=?1 THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) FROM friend_requests WHERE from_uid=?1 OR to_uid=?1",params![s.uid.as_str(),now-3_600_000],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).db()?;
        if counts.0 >= 10000 || counts.1 >= 30 || counts.2 >= 100 {
            return Err(fail(ErrorCode::RateLimited, "好友申请过多，请稍后再试"));
        }
        let (recipient_total,recipient_pending):(i64,i64)=self.conn.query_row("SELECT COUNT(*),COALESCE(SUM(CASE WHEN to_uid=?1 AND status='pending' THEN 1 ELSE 0 END),0) FROM friend_requests WHERE to_uid=?1 OR from_uid=?1",params![b.to.uid.as_str()],|r|Ok((r.get(0)?,r.get(1)?))).db()?;
        if recipient_pending >= 100 || recipient_total >= 10000 {
            return Err(fail(
                ErrorCode::RateLimited,
                "对方待处理申请较多，请稍后再试",
            ));
        }
        let tx = self.conn.unchecked_transaction().db()?;
        let log = Self::append_log(&tx, now, "friend_request", None)?;
        let record = StoredFriendRequest {
            envelope: envelope.clone(),
            log,
            created_ms: now,
            expires_ms: now.saturating_add(REQUEST_TTL_MS),
            updated_ms: now,
            status: Status::Pending,
            conversation: None,
        };
        tx.execute("INSERT INTO friend_requests(request,from_uid,to_uid,pair_key,status,created_ms,expires_ms,updated_ms,record) VALUES(?1,?2,?3,?4,'pending',?5,?6,?5,?7)",params![b.id.as_str(),b.from.uid.as_str(),b.to.uid.as_str(),key,now,record.expires_ms,to_json(&record)?]).db()?;
        tx.commit().db()?;
        self.last_log = log;
        effects.pushes.extend(changed(&record).pushes);
        tracing::info!(request=%b.id,"好友申请已提交，等待对方批准");
        Ok((Response::FriendRequest { request: Box::new(record) }, effects))
    }

    pub(crate) fn expire_friend_requests(&mut self, now: i64) -> Result<Effects, WireError> {
        let values:Vec<String>=self.conn.prepare_cached("SELECT record FROM friend_requests WHERE status='pending' AND expires_ms<=?1 LIMIT 500").db()?.query_map(params![now],|r|r.get(0)).db()?.collect::<Result<_,_>>().db()?;
        let tx = self.conn.unchecked_transaction().db()?;
        let mut effects = Effects::default();
        for json in values {
            let mut r: StoredFriendRequest = from_json(&json)?;
            r.status = Status::Expired;
            r.updated_ms = now;
            Self::write_friend_record(&tx, &r)?;
            effects.pushes.extend(changed(&r).pushes);
        }
        tx.commit().db()?;
        Ok(effects)
    }

    pub(crate) fn friend_requests(
        &mut self,
        s: &Session,
        before: Option<u64>,
        limit: u32,
        incoming: bool,
        peer: Option<&Uid>,
        now: i64,
    ) -> OpResult {
        self.require_active(s)?;
        if !(1..=100).contains(&limit) || before.is_some_and(|v| v == 0 || v > i64::MAX as u64) {
            return Err(fail(ErrorCode::BadRequest, "好友申请分页无效"));
        }
        let mut effects = self.expire_friend_requests(now)?;
        let sql = if incoming {
            "SELECT seq,record FROM friend_requests WHERE to_uid=?1 AND (?2 IS NULL OR seq<?2) AND (?3 IS NULL OR from_uid=?3) ORDER BY seq DESC LIMIT ?4"
        } else {
            "SELECT seq,record FROM friend_requests WHERE from_uid=?1 AND (?2 IS NULL OR seq<?2) AND (?3 IS NULL OR to_uid=?3) ORDER BY seq DESC LIMIT ?4"
        };
        let mut rows: Vec<(i64, String)> = self
            .conn
            .prepare_cached(sql)
            .db()?
            .query_map(
                params![
                    s.uid.as_str(),
                    before.map(|v| v as i64),
                    peer.map(Uid::as_str),
                    limit + 1
                ],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .db()?
            .collect::<Result<_, _>>()
            .db()?;
        let more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let next = if more {
            rows.last().map(|r| r.0 as u64)
        } else {
            None
        };
        let mut requests: Vec<StoredFriendRequest> = rows
            .into_iter()
            .map(|(_, j)| from_json(&j))
            .collect::<Result<Vec<_>, _>>()?;
        for r in &mut requests {
            if self.refresh_friend_record(r, now)? {
                effects.pushes.extend(changed(r).pushes);
            }
        }
        let pending:u32=self.conn.query_row("SELECT COUNT(*) FROM friend_requests WHERE to_uid=?1 AND status='pending' AND expires_ms>?2",params![s.uid.as_str(),now],|r|r.get(0)).db()?;
        Ok((
            Response::FriendRequests {
                requests,
                next,
                pending,
            },
            effects,
        ))
    }

    pub(crate) fn friend_request_get(&mut self, s: &Session, id: &RequestId, now: i64) -> OpResult {
        let mut r = self.friend_record(s, id)?;
        let mut effects = Effects::default();
        if self.refresh_friend_record(&mut r, now)? {
            effects = changed(&r);
        }
        Ok((Response::FriendRequest { request: Box::new(r) }, effects))
    }

    pub(crate) fn friend_request_close(
        &mut self,
        s: &Session,
        id: &RequestId,
        withdraw: bool,
        now: i64,
    ) -> OpResult {
        let mut r = self.friend_record(s, id)?;
        let allowed = if withdraw {
            &r.envelope.body.from.uid
        } else {
            &r.envelope.body.to.uid
        };
        if allowed != &s.uid {
            return Err(fail(ErrorCode::Forbidden, "不能处理其他人的好友申请"));
        }
        if r.status != Status::Pending {
            return Ok((Response::FriendRequest { request: Box::new(r) }, Effects::default()));
        }
        r.status = if r.expires_ms <= now {
            Status::Expired
        } else if withdraw {
            Status::Withdrawn
        } else {
            Status::Rejected
        };
        r.updated_ms = now;
        let tx = self.conn.unchecked_transaction().db()?;
        Self::write_friend_record(&tx, &r)?;
        tx.commit().db()?;
        tracing::info!(request=%id,status=?r.status,"好友申请状态已更新");
        let effects = changed(&r);
        Ok((Response::FriendRequest { request: Box::new(r) }, effects))
    }

    pub(crate) fn friend_request_accept(
        &mut self,
        s: &Session,
        id: &RequestId,
        link: &ConvLink,
        keys: &ConvKeyBundle,
        now: i64,
    ) -> OpResult {
        let mut r = self.friend_record(s, id)?;
        if r.envelope.body.to.uid != s.uid {
            return Err(fail(ErrorCode::Forbidden, "只有接收方可以同意申请"));
        }
        if r.status != Status::Pending {
            return Ok((Response::FriendRequest { request: Box::new(r) }, Effects::default()));
        }
        if self.refresh_friend_record(&mut r, now)? {
            let effects = changed(&r);
            return Ok((Response::FriendRequest { request: Box::new(r) }, effects));
        }
        let body = link
            .parse()
            .map_err(|e| fail(ErrorCode::Invalid, e.to_string()))?;
        let ConvChange::Create {
            kind: ConvKind::Dm,
            members,
            ..
        } = &body.change
        else {
            return Err(fail(ErrorCode::Invalid, "批准只能建立双方私聊"));
        };
        if members.len() != 2
            || !members.contains(&r.envelope.body.from)
            || !members.contains(&r.envelope.body.to)
        {
            return Err(fail(ErrorCode::Forbidden, "私聊成员与申请不一致"));
        }
        r.status = Status::Accepted;
        r.updated_ms = now;
        r.conversation = Some(body.conv.clone());
        let (_, mut effects) = self.create_approved_conv(s, link, keys, now, Some(&r))?;
        effects.pushes.extend(changed(&r).pushes);
        Ok((Response::FriendRequest { request: Box::new(r) }, effects))
    }
}
