//! 登录、账号签名链、好友链接与资料。

use std::collections::BTreeSet;

use friend_im_core::account::{AccountChange, AccountLink, AccountState, check_device_name};
use friend_im_core::bytes::Bytes;
use friend_im_core::crypto;
use friend_im_core::ids::{DeviceId, Uid};
use friend_im_core::keys::DevicePublic;
use friend_im_core::limits;
use friend_im_core::wire::{
    AccountEntry, AccountInfo, AccountKeyBundle, DeviceInfo, DeviceKind, DeviceStatus, ErrorCode, Event, GenerationBox, GenerationWrap, Push, Response, SyncEvent,
    Welcome, WireError,
};
use rusqlite::{OptionalExtension, params};

use crate::cindy::CindyIdentity;
use crate::store::{DbResult, DeviceRow, DeviceState, Effects, Store, Target, fail, from_json, internal, to_json};

/// 一条连接登录后的身份。
#[derive(Clone, Debug)]
pub struct Session {
    /// 账号。
    pub uid: Uid,
    /// 电脑。
    pub device: DeviceId,
}

/// 登录方式（Cindy 凭证已在锁外核对完）。
pub enum LoginAuth {
    /// 核对过的 Cindy 身份。
    Cindy(CindyIdentity),
    /// 服务器发的登录凭证的摘要。
    Session([u8; 32]),
}

/// 一台账号最多同时有几台还没批准的电脑：超过时删掉最早的那台，防止被刷出大量电脑记录。
const PENDING_DEVICES_MAX: usize = 5;

/// 一个操作的结果：回执 + 要发的推送。
pub type OpResult = Result<(Response, Effects), WireError>;

pub(crate) fn session_hash(token: &[u8]) -> [u8; 32] {
    crypto::hash("friend-im/v1/session", token)
}

impl Store {
    /// 登录：Cindy 凭证（第一次）或登录凭证。返回会话身份、回执与要补发的推送。
    pub fn login(&mut self, auth: LoginAuth, keys: DevicePublic, name: &str, kind: DeviceKind, now: i64) -> Result<(Session, Welcome, Effects), WireError> {
        check_device_name(name).map_err(|e| fail(ErrorCode::BadRequest, e.to_string()))?;
        keys.check().map_err(|e| fail(ErrorCode::BadRequest, e.to_string()))?;
        let device_id = keys.id();
        let mut effects = Effects::default();
        let (uid, session_token) = match auth {
            LoginAuth::Cindy(identity) => {
                let uid = Uid::from_cindy(&identity.issuer, &identity.member);
                let tx = self.conn.unchecked_transaction().db()?;
                tx.execute(
                    "INSERT INTO accounts(uid, name, created_ms) VALUES(?1, ?2, ?3) ON CONFLICT(uid) DO UPDATE SET name = excluded.name",
                    params![uid.as_str(), identity.name, now],
                )
                .db()?;
                match self.device(&device_id)? {
                    Some(row) if row.uid != uid => return Err(fail(ErrorCode::Forbidden, "这台电脑属于另一个账号")),
                    Some(row) if row.state == DeviceState::Removed => {
                        return Err(fail(ErrorCode::DeviceRemoved, "这台电脑已被移出账号，需要重新生成电脑钥匙"));
                    }
                    Some(_) => {}
                    None => {
                        let pending = self.devices_of(&uid, DeviceState::Pending)?;
                        if pending.len() >= PENDING_DEVICES_MAX {
                            tx.execute("DELETE FROM devices WHERE device = ?1", params![pending[0].device.as_str()]).db()?;
                            tracing::info!(uid = %uid, "未批准的电脑太多，删掉最早的一台");
                        }
                        tx.execute(
                            "INSERT INTO devices(device, uid, sign_pub, enc_pub, name, kind, status, created_ms) VALUES(?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7)",
                            params![device_id.as_str(), uid.as_str(), keys.sign.0.to_vec(), keys.enc.0.to_vec(), name, kind.as_str(), now],
                        )
                        .db()?;
                    }
                }
                let token: [u8; 32] = crypto::random();
                tx.execute(
                    "UPDATE devices SET session_hash = ?1, last_seen_ms = ?2 WHERE device = ?3",
                    params![session_hash(&token).to_vec(), now, device_id.as_str()],
                )
                .db()?;
                tx.commit().db()?;
                (uid, Some(Bytes(token.to_vec())))
            }
            LoginAuth::Session(hash) => {
                let found: Option<(String, String)> = self
                    .conn
                    .query_row("SELECT device, uid FROM devices WHERE session_hash = ?1", params![hash.to_vec()], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })
                    .optional()
                    .db()?;
                let Some((device, uid)) = found else {
                    // 移出时登录凭证已清掉；握手已用电脑钥匙签过名，是这台电脑本人，照实告诉它被移出了。
                    if self.device(&device_id)?.is_some_and(|row| row.state == DeviceState::Removed) {
                        return Err(fail(ErrorCode::DeviceRemoved, "这台电脑已被移出账号"));
                    }
                    return Err(fail(ErrorCode::Unauthorized, "登录凭证无效，请重新登录"));
                };
                if device != device_id.as_str() {
                    return Err(fail(ErrorCode::Unauthorized, "登录凭证不属于这台电脑"));
                }
                self.conn
                    .execute("UPDATE devices SET last_seen_ms = ?1 WHERE device = ?2", params![now, device])
                    .db()?;
                (Uid::parse(&uid).map_err(internal)?, None)
            }
        };
        self.conn
            .execute("UPDATE devices SET kind = ?1 WHERE device = ?2", params![kind.as_str(), device_id.as_str()])
            .db()?;
        let account = self.account(&uid)?.ok_or_else(|| internal("登录后找不到账号"))?;
        let row = self.device(&device_id)?.ok_or_else(|| internal("登录后找不到电脑"))?;
        let status = match (account.provisioned, row.state) {
            (_, DeviceState::Removed) => return Err(fail(ErrorCode::DeviceRemoved, "这台电脑已被移出账号")),
            (false, _) => DeviceStatus::Unprovisioned,
            (true, DeviceState::Pending) => DeviceStatus::Pending,
            (true, DeviceState::Active) => DeviceStatus::Active,
        };
        let chain_seq = self.current_state(&uid)?.map(|s| s.seq).unwrap_or(0);
        if status == DeviceStatus::Pending {
            // 断线重连的新电脑：旧电脑已经回应过的话，把回应再推一次。
            self.replay_join_answer(&device_id, now, &mut effects)?;
        }
        let welcome = Welcome {
            reading: true,
            uid: uid.clone(),
            name: account.name,
            epoch: account.epoch,
            status,
            session: session_token,
            chain_seq,
            rotation_needed: account.rotation_needed,
            sync_head: self.last_log,
        };
        Ok((Session { uid, device: device_id }, welcome, effects))
    }

    /// 要求这台电脑已批准；返回它的记录。
    pub(crate) fn require_active(&self, s: &Session) -> Result<DeviceRow, WireError> {
        let row = self.device(&s.device)?.ok_or_else(|| fail(ErrorCode::Unauthorized, "找不到这台电脑"))?;
        match row.state {
            DeviceState::Active => Ok(row),
            DeviceState::Pending => Err(fail(ErrorCode::NotApproved, "这台电脑还没被批准")),
            DeviceState::Removed => Err(fail(ErrorCode::DeviceRemoved, "这台电脑已被移出账号")),
        }
    }

    pub(crate) fn require_pending(&self, s: &Session) -> Result<DeviceRow, WireError> {
        let row = self.device(&s.device)?.ok_or_else(|| fail(ErrorCode::Unauthorized, "找不到这台电脑"))?;
        match row.state {
            DeviceState::Pending => Ok(row),
            DeviceState::Active => Err(fail(ErrorCode::Conflict, "这台电脑已经批准过了")),
            DeviceState::Removed => Err(fail(ErrorCode::DeviceRemoved, "这台电脑已被移出账号")),
        }
    }

    /// 核对一份账号钥匙封装：代数对、收件人正好是期望的那几把钥匙、每把一份。
    pub(crate) fn check_account_bundle(bundle: &AccountKeyBundle, generation: u32, expected: &BTreeSet<DeviceId>, need_prev: bool) -> Result<(), WireError> {
        if bundle.generation != generation {
            return Err(fail(ErrorCode::Invalid, "账号钥匙封装的代数不对"));
        }
        let got: BTreeSet<&DeviceId> = bundle.boxes.iter().map(|b| &b.device).collect();
        if got.len() != bundle.boxes.len() || got != expected.iter().collect() {
            return Err(fail(ErrorCode::Invalid, "账号钥匙封装的收件人不对"));
        }
        if bundle.boxes.iter().any(|b| b.sealed.ct.len() > 128) {
            return Err(fail(ErrorCode::Invalid, "账号钥匙封装太大"));
        }
        if need_prev != bundle.prev.is_some() || bundle.prev.as_ref().is_some_and(|p| p.len() > 128) {
            return Err(fail(ErrorCode::Invalid, "旧钥匙链不对"));
        }
        Ok(())
    }

    pub(crate) fn write_account_bundle(conn: &rusqlite::Connection, uid: &Uid, epoch: u32, bundle: &AccountKeyBundle) -> Result<(), WireError> {
        for b in &bundle.boxes {
            conn.execute(
                "INSERT OR REPLACE INTO account_keys(uid, epoch, generation, device, sealed) VALUES(?1, ?2, ?3, ?4, ?5)",
                params![uid.as_str(), epoch as i64, bundle.generation as i64, b.device.as_str(), to_json(&b.sealed)?],
            )
            .db()?;
        }
        if let Some(prev) = &bundle.prev {
            conn.execute(
                "INSERT OR REPLACE INTO account_prev(uid, epoch, generation, wrapped) VALUES(?1, ?2, ?3, ?4)",
                params![uid.as_str(), epoch as i64, bundle.generation as i64, prev.0.clone()],
            )
            .db()?;
        }
        Ok(())
    }

    /// 写一环账号签名链：链表、日志、扇出给本人和聊过的人；返回日志序号与同步事件。
    #[allow(clippy::too_many_arguments)] // 一环签名链落库要的全部信息，拆成结构体反而更难读
    pub(crate) fn write_account_link(&self, conn: &rusqlite::Connection, uid: &Uid, epoch: u32, seq: u64, link: &AccountLink, now: i64, recipients: &[Uid]) -> Result<SyncEvent, WireError> {
        let event = Event::AccountLink {
            uid: uid.clone(),
            epoch,
            link: link.clone(),
        };
        let log = Self::append_log(conn, now, "account_link", Some(&to_json(&event)?))?;
        conn.execute(
            "INSERT INTO account_links(uid, epoch, seq, link, log) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![uid.as_str(), epoch as i64, seq as i64, to_json(link)?, log as i64],
        )
        .db()?;
        Self::fan_out(conn, log, recipients)?;
        Ok(SyncEvent { log, ts_ms: now, event })
    }

    /// 开通：第一台电脑提交第一环和账号钥匙第一代。
    pub fn provision(&mut self, s: &Session, eldest: &AccountLink, keys: &AccountKeyBundle, now: i64) -> OpResult {
        let account = self.account(&s.uid)?.ok_or_else(|| internal("找不到账号"))?;
        if account.provisioned {
            return Err(fail(ErrorCode::Conflict, "账号已经开通过了"));
        }
        self.require_pending(s)?;
        let log = self.last_log + 1;
        let state = AccountState::eldest(&s.uid, account.epoch, eldest, log).map_err(|e| fail(ErrorCode::Invalid, e.to_string()))?;
        if !state.is_active(&s.device) {
            return Err(fail(ErrorCode::Invalid, "第一环里的电脑必须是这台电脑"));
        }
        let expected: BTreeSet<DeviceId> = [s.device.clone(), state.recovery.id()].into();
        Self::check_account_bundle(keys, 1, &expected, false)?;
        let tx = self.conn.unchecked_transaction().db()?;
        let event = self.write_account_link(&tx, &s.uid, account.epoch, 1, eldest, now, std::slice::from_ref(&s.uid))?;
        if event.log != log {
            return Err(internal("日志序号与预期不符"));
        }
        Self::write_account_bundle(&tx, &s.uid, account.epoch, keys)?;
        tx.execute("UPDATE accounts SET provisioned = 1, rotation_needed = 0 WHERE uid = ?1", params![s.uid.as_str()])
            .db()?;
        tx.execute("UPDATE devices SET status = 'active', joined_ms = ?1 WHERE device = ?2", params![now, s.device.as_str()])
            .db()?;
        tx.commit().db()?;
        self.last_log = log;
        self.accounts.entry(s.uid.clone()).or_default().epochs.insert(account.epoch, state);
        tracing::info!(uid = %s.uid, log, "账号开通");
        let mut effects = Effects::default();
        effects.approve.push(s.device.clone());
        effects.push(Target::Account(s.uid.clone()), Push::Event { event });
        Ok((Response::Appended { log }, effects))
    }

    /// 接账号签名链：移除电脑、换代、换恢复密钥、退出登录。
    pub fn append_account(&mut self, s: &Session, links: &[AccountLink], keys: Option<&AccountKeyBundle>, now: i64) -> OpResult {
        self.require_active(s)?;
        if links.is_empty() || links.len() > limits::ACCOUNT_LINKS_PER_REQUEST {
            return Err(fail(ErrorCode::BadRequest, "一次接的环数不对"));
        }
        let account = self.account(&s.uid)?.ok_or_else(|| internal("找不到账号"))?;
        let mut state = self.current_state(&s.uid)?.ok_or_else(|| internal("已批准的电脑却没有签名链"))?;
        let before_generation = state.current_key().generation;
        let mut removed = Vec::new();
        for (i, link) in links.iter().enumerate() {
            let body = link.parse().map_err(|e| fail(ErrorCode::Invalid, e.to_string()))?;
            if matches!(body.change, AccountChange::AddDevice { .. }) {
                return Err(fail(ErrorCode::BadRequest, "加电脑走批准流程"));
            }
            if let AccountChange::RemoveDevice { device, .. } = &body.change {
                removed.push(device.clone());
            }
            state.apply(link, self.last_log + 1 + i as u64).map_err(|e| fail(ErrorCode::Invalid, e.to_string()))?;
        }
        let after_generation = state.current_key().generation;
        match (after_generation - before_generation, keys) {
            (0, None) => {}
            (1, Some(bundle)) => {
                let mut expected: BTreeSet<DeviceId> = state.active_devices().map(|(id, _)| id.clone()).collect();
                expected.insert(state.recovery.id());
                Self::check_account_bundle(bundle, after_generation, &expected, true)?;
            }
            (0, Some(_)) => return Err(fail(ErrorCode::BadRequest, "没有换代却带了钥匙封装")),
            (1, None) => return Err(fail(ErrorCode::Invalid, "换代必须带新一代的封装")),
            _ => return Err(fail(ErrorCode::BadRequest, "一个请求只能换一代")),
        }
        let recipients = self.contacts(&s.uid)?;
        let first_seq = state.seq + 1 - links.len() as u64;
        let tx = self.conn.unchecked_transaction().db()?;
        let mut events = Vec::new();
        for (i, link) in links.iter().enumerate() {
            events.push(self.write_account_link(&tx, &s.uid, account.epoch, first_seq + i as u64, link, now, &recipients)?);
        }
        if let Some(bundle) = keys {
            Self::write_account_bundle(&tx, &s.uid, account.epoch, bundle)?;
            // 账号钥匙换了代：所在的每个会话都要在下一次发消息前换代（被移除的电脑拿得到旧代）。
            tx.execute(
                "UPDATE convs SET rotation_needed = 1 WHERE conv IN (SELECT conv FROM conv_members WHERE uid = ?1 AND left_log IS NULL)",
                params![s.uid.as_str()],
            )
            .db()?;
        }
        for device in &removed {
            tx.execute("UPDATE devices SET status = 'removed', session_hash = NULL WHERE device = ?1", params![device.as_str()])
                .db()?;
        }
        tx.execute(
            "UPDATE accounts SET rotation_needed = ?1 WHERE uid = ?2",
            params![i64::from(state.rotation_pending), s.uid.as_str()],
        )
        .db()?;
        tx.commit().db()?;
        let last = events.last().map(|e| e.log).unwrap_or(self.last_log);
        self.last_log = last;
        self.accounts.entry(s.uid.clone()).or_default().epochs.insert(account.epoch, state);
        tracing::info!(uid = %s.uid, removed = removed.len(), rotated = keys.is_some(), "账号签名链接了 {} 环", links.len());
        let mut effects = Effects::default();
        for event in events {
            for uid in &recipients {
                effects.push(Target::Account(uid.clone()), Push::Event { event: event.clone() });
            }
        }
        for device in removed {
            effects.push(Target::Device(device.clone()), Push::DeviceRemoved);
            effects.close.push(device);
        }
        Ok((Response::Appended { log: last }, effects))
    }

    /// 重置账号（契约 S7）。
    pub fn reset(&mut self, s: &Session, eldest: &AccountLink, keys: &AccountKeyBundle, now: i64) -> OpResult {
        let account = self.account(&s.uid)?.ok_or_else(|| internal("找不到账号"))?;
        if !account.provisioned {
            return Err(fail(ErrorCode::Conflict, "账号还没开通，直接开通即可"));
        }
        let row = self.device(&s.device)?.ok_or_else(|| fail(ErrorCode::Unauthorized, "找不到这台电脑"))?;
        if row.state == DeviceState::Removed {
            return Err(fail(ErrorCode::DeviceRemoved, "这台电脑已被移出账号"));
        }
        let epoch = account.epoch + 1;
        let log = self.last_log + 1;
        let state = AccountState::eldest(&s.uid, epoch, eldest, log).map_err(|e| fail(ErrorCode::Invalid, e.to_string()))?;
        if !state.is_active(&s.device) {
            return Err(fail(ErrorCode::Invalid, "新一轮第一环里的电脑必须是这台电脑"));
        }
        let expected: BTreeSet<DeviceId> = [s.device.clone(), state.recovery.id()].into();
        Self::check_account_bundle(keys, 1, &expected, false)?;
        // 先算通知对象：重置前一起聊天的人都要知道（群成员据此补签“已移出”，私聊对方看到“重置了账号”）。
        let recipients = self.contacts(&s.uid)?;
        let others: Vec<DeviceRow> = [DeviceState::Pending, DeviceState::Active]
            .into_iter()
            .map(|st| self.devices_of(&s.uid, st))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .filter(|d| d.device != s.device)
            .collect();
        let open_joins = self.open_joins_of(&s.uid)?;
        let tx = self.conn.unchecked_transaction().db()?;
        let event = self.write_account_link(&tx, &s.uid, epoch, 1, eldest, now, &recipients)?;
        if event.log != log {
            return Err(internal("日志序号与预期不符"));
        }
        Self::write_account_bundle(&tx, &s.uid, epoch, keys)?;
        tx.execute(
            "UPDATE accounts SET epoch = ?1, rotation_needed = 0, friend_token = NULL WHERE uid = ?2",
            params![epoch as i64, s.uid.as_str()],
        )
        .db()?;
        for d in &others {
            tx.execute("UPDATE devices SET status = 'removed', session_hash = NULL WHERE device = ?1", params![d.device.as_str()])
                .db()?;
        }
        tx.execute("UPDATE devices SET status = 'active', joined_ms = ?1 WHERE device = ?2", params![now, s.device.as_str()])
            .db()?;
        // 自动退出所有群；私聊保留，等任一方用新钥匙接上。
        tx.execute(
            "UPDATE conv_members SET left_log = ?1 WHERE uid = ?2 AND left_log IS NULL AND conv IN (SELECT conv FROM convs WHERE kind = 'group')",
            params![log as i64, s.uid.as_str()],
        )
        .db()?;
        tx.execute(
            "UPDATE convs SET rotation_needed = 1 WHERE conv IN (SELECT conv FROM conv_members WHERE uid = ?1)",
            params![s.uid.as_str()],
        )
        .db()?;
        tx.execute("UPDATE joins SET open = 0 WHERE uid = ?1 AND open = 1", params![s.uid.as_str()]).db()?;
        tx.commit().db()?;
        self.last_log = log;
        self.accounts.entry(s.uid.clone()).or_default().epochs.insert(epoch, state);
        tracing::info!(uid = %s.uid, epoch, "账号重置");
        let mut effects = Effects::default();
        for uid in &recipients {
            effects.push(Target::Account(uid.clone()), Push::Event { event: event.clone() });
        }
        for request in open_joins {
            effects.push(
                Target::Device(request.1),
                Push::JoinClosed {
                    request: request.0,
                    reason: friend_im_core::wire::JoinCloseReason::Cancelled,
                },
            );
        }
        for d in others {
            effects.push(Target::Device(d.device.clone()), Push::DeviceRemoved);
            effects.close.push(d.device);
        }
        effects.approve.push(s.device.clone());
        Ok((Response::Appended { log }, effects))
    }

    /// 取封装给这台电脑的账号钥匙（当前一轮）和旧钥匙链。
    pub fn get_account_keys(&mut self, s: &Session) -> OpResult {
        self.require_active(s)?;
        let account = self.account(&s.uid)?.ok_or_else(|| internal("找不到账号"))?;
        self.account_keys_for(&s.uid, account.epoch, &s.device)
    }

    /// 新电脑用恢复密钥加入前：取封装给当前恢复密钥的账号钥匙。只有恢复密钥打得开，给还没批准的电脑也不泄露什么。
    pub fn get_recovery_keys(&mut self, s: &Session) -> OpResult {
        let row = self.device(&s.device)?.ok_or_else(|| fail(ErrorCode::Unauthorized, "找不到这台电脑"))?;
        if row.state == DeviceState::Removed {
            return Err(fail(ErrorCode::DeviceRemoved, "这台电脑已被移出账号"));
        }
        let account = self.account(&s.uid)?.ok_or_else(|| internal("找不到账号"))?;
        let state = self.current_state(&s.uid)?.ok_or_else(|| fail(ErrorCode::Conflict, "账号还没开通"))?;
        self.account_keys_for(&s.uid, account.epoch, &state.recovery.id())
    }

    fn account_keys_for(&self, uid: &Uid, epoch: u32, device: &DeviceId) -> OpResult {
        let boxes: Vec<GenerationBox> = {
            let mut stmt = self
                .conn
                .prepare_cached("SELECT generation, sealed FROM account_keys WHERE uid = ?1 AND epoch = ?2 AND device = ?3 ORDER BY generation")
                .db()?;
            let rows = stmt
                .query_map(params![uid.as_str(), epoch as i64, device.as_str()], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
                .db()?;
            let mut out = Vec::new();
            for row in rows {
                let (generation, sealed) = row.db()?;
                out.push(GenerationBox {
                    generation: generation as u32,
                    sealed: from_json(&sealed)?,
                });
            }
            out
        };
        let prev = self.account_prev(uid, epoch)?;
        Ok((Response::AccountKeys { epoch, boxes, prev }, Effects::default()))
    }

    fn account_prev(&self, uid: &Uid, epoch: u32) -> Result<Vec<GenerationWrap>, WireError> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT generation, wrapped FROM account_prev WHERE uid = ?1 AND epoch = ?2 ORDER BY generation")
            .db()?;
        let rows = stmt
            .query_map(params![uid.as_str(), epoch as i64], |r| {
                Ok(GenerationWrap {
                    generation: r.get::<_, i64>(0)? as u32,
                    wrapped: Bytes(r.get(1)?),
                })
            })
            .db()?;
        rows.collect::<Result<_, _>>().db()
    }

    /// 取某账号的签名链。还没批准的电脑只能取自己账号的。
    pub fn get_chain(&mut self, s: &Session, uid: &Uid, epoch: Option<u32>, after: u64) -> OpResult {
        let row = self.device(&s.device)?.ok_or_else(|| fail(ErrorCode::Unauthorized, "找不到这台电脑"))?;
        match row.state {
            DeviceState::Active => {}
            DeviceState::Pending if uid == &s.uid => {}
            DeviceState::Pending => return Err(fail(ErrorCode::NotApproved, "这台电脑还没被批准")),
            DeviceState::Removed => return Err(fail(ErrorCode::DeviceRemoved, "这台电脑已被移出账号")),
        }
        let account = self.account(uid)?.ok_or_else(|| fail(ErrorCode::NotFound, "没有这个账号"))?;
        let epoch = epoch.unwrap_or(account.epoch);
        if epoch > account.epoch {
            return Err(fail(ErrorCode::NotFound, "没有这一轮"));
        }
        let mut stmt = self
            .conn
            .prepare_cached("SELECT link, log FROM account_links WHERE uid = ?1 AND epoch = ?2 AND seq > ?3 ORDER BY seq")
            .db()?;
        let rows = stmt
            .query_map(params![uid.as_str(), epoch as i64, after as i64], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
            .db()?;
        let mut entries = Vec::new();
        for row in rows {
            let (link, log) = row.db()?;
            entries.push(AccountEntry {
                log: log as u64,
                link: from_json(&link)?,
            });
        }
        Ok((
            Response::Chain {
                uid: uid.clone(),
                epoch,
                current_epoch: account.epoch,
                entries,
            },
            Effects::default(),
        ))
    }

    /// 本账号现在的电脑（“我的电脑”）。
    pub fn list_devices(&mut self, s: &Session, online: &dyn Fn(&DeviceId) -> bool) -> OpResult {
        self.require_active(s)?;
        let devices = self.device_infos(&s.uid, online)?;
        Ok((Response::Devices { devices }, Effects::default()))
    }

    /// 一个账号已加入的电脑：名字、时间、此刻在不在线（“我的电脑”，新电脑请求批准时也交回这一份）。
    pub(crate) fn device_infos(&self, uid: &Uid, online: &dyn Fn(&DeviceId) -> bool) -> Result<Vec<DeviceInfo>, WireError> {
        Ok(self
            .devices_of(uid, DeviceState::Active)?
            .into_iter()
            .map(|d| DeviceInfo {
                online: online(&d.device),
                id: d.device,
                name: d.name,
                kind: d.kind,
                joined_ms: d.joined_ms,
                last_seen_ms: d.last_seen_ms,
            })
            .collect())
    }

    /// 我的好友链接编号（没有就新建）。
    pub fn get_friend_link(&mut self, s: &Session, rotate: bool) -> OpResult {
        self.require_active(s)?;
        let current: Option<Vec<u8>> = self
            .conn
            .query_row("SELECT friend_token FROM accounts WHERE uid = ?1", params![s.uid.as_str()], |r| r.get(0))
            .db()?;
        let token = match current {
            Some(token) if !rotate => token,
            _ => {
                let token = crypto::random::<16>().to_vec();
                self.conn
                    .execute("UPDATE accounts SET friend_token = ?1 WHERE uid = ?2", params![token, s.uid.as_str()])
                    .db()?;
                token
            }
        };
        Ok((Response::FriendLink { token: Bytes(token) }, Effects::default()))
    }

    fn account_info(&mut self, uid: &Uid) -> Result<Option<AccountInfo>, WireError> {
        let Some(account) = self.account(uid)? else {
            return Ok(None);
        };
        let eldest = self.current_state(uid)?.map(|s| s.eldest);
        Ok(Some(AccountInfo {
            uid: uid.clone(),
            name: account.name,
            epoch: account.epoch,
            eldest,
        }))
    }

    /// 按好友链接编号查对方。
    pub fn resolve_friend_link(&mut self, s: &Session, token: &Bytes) -> OpResult {
        self.require_active(s)?;
        if token.len() != 16 {
            return Err(fail(ErrorCode::NotFound, "好友链接无效或已失效"));
        }
        let uid: Option<String> = self
            .conn
            .query_row("SELECT uid FROM accounts WHERE friend_token = ?1", params![token.0.clone()], |r| r.get(0))
            .optional()
            .db()?;
        let uid = uid.ok_or_else(|| fail(ErrorCode::NotFound, "好友链接无效或已失效"))?;
        let info = self
            .account_info(&Uid::parse(&uid).map_err(internal)?)?
            .filter(|i| i.eldest.is_some())
            .ok_or_else(|| fail(ErrorCode::NotFound, "好友链接无效或已失效"))?;
        Ok((Response::Profiles { profiles: vec![info] }, Effects::default()))
    }

    /// 查一批账号的名字与身份指纹。
    pub fn get_profiles(&mut self, s: &Session, uids: &[Uid]) -> OpResult {
        self.require_active(s)?;
        if uids.len() > limits::PROFILES_PER_REQUEST {
            return Err(fail(ErrorCode::LimitExceeded, "一次查的账号太多"));
        }
        let mut profiles = Vec::new();
        for uid in uids {
            if let Some(info) = self.account_info(uid)? {
                profiles.push(info);
            }
        }
        Ok((Response::Profiles { profiles }, Effects::default()))
    }
}
