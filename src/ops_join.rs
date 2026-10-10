//! 新电脑批准（契约 S2、S3、S20、S21）。服务器只转发 SPAKE2 消息和确认码，自己算不出握手钥匙。
//!
//! - 每个账号同时只有 1 个请求、每小时最多 10 个；10 分钟没批准自动作废；输错 5 次作废。
//! - 批准 = 一个请求里提交“加电脑”这一环 + 给新电脑的账号钥匙封装，一个事务写入。

use std::collections::BTreeSet;

use friend_im_core::account::{AccountChange, AccountLink, ProofRole, check_proof};
use friend_im_core::bytes::{Bytes, Fixed};
use friend_im_core::ids::{DeviceId, RequestId, Uid};
use friend_im_core::limits;
use friend_im_core::wire::{AccountKeyBundle, ErrorCode, JoinCloseReason, JoinRequestInfo, Push, Response, WireError};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::ops_account::{OpResult, Session};
use crate::store::{DbResult, Effects, Store, Target, fail, from_json, internal, to_json};

/// SPAKE2 消息的长度上限（Ed25519 群的消息是 33 字节，留余量）。
const PAKE_MAX: usize = 256;

#[derive(Serialize, Deserialize)]
struct Answer {
    attempt: u32,
    pake: Bytes,
    confirm: Fixed<32>,
}

struct JoinRow {
    request: RequestId,
    uid: Uid,
    device: DeviceId,
    created_ms: i64,
    expires_ms: i64,
    attempt: u32,
    pake: Bytes,
    proof: Fixed<64>,
    responder: Option<DeviceId>,
    answer: Option<Answer>,
    open: bool,
}

impl Store {
    fn join_row(&self, request: &RequestId) -> Result<Option<JoinRow>, WireError> {
        let row = self
            .conn
            .query_row(
                "SELECT uid, device, created_ms, expires_ms, attempt, pake, proof, responder, answer, open FROM joins WHERE request = ?1",
                params![request.as_str()],
                |r| {
                    Ok((
                        (r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?, r.get::<_, i64>(3)?, r.get::<_, i64>(4)?),
                        (
                            r.get::<_, Vec<u8>>(5)?,
                            r.get::<_, Vec<u8>>(6)?,
                            r.get::<_, Option<String>>(7)?,
                            r.get::<_, Option<String>>(8)?,
                            r.get::<_, i64>(9)?,
                        ),
                    ))
                },
            )
            .optional()
            .db()?;
        row.map(|((uid, device, created_ms, expires_ms, attempt), (pake, proof, responder, answer, open))| {
            Ok(JoinRow {
                request: request.clone(),
                uid: Uid::parse(&uid).map_err(internal)?,
                device: DeviceId::parse(&device).map_err(internal)?,
                created_ms,
                expires_ms,
                attempt: attempt as u32,
                pake: Bytes(pake),
                proof: Fixed::from_slice(&proof).ok_or_else(|| internal("证明长度不对"))?,
                responder: responder.map(|r| DeviceId::parse(&r)).transpose().map_err(internal)?,
                answer: answer.map(|a| from_json(&a)).transpose()?,
                open: open != 0,
            })
        })
        .transpose()
    }

    /// 某账号还开着的请求：（请求，新电脑）。
    pub(crate) fn open_joins_of(&self, uid: &Uid) -> Result<Vec<(RequestId, DeviceId)>, WireError> {
        let mut stmt = self.conn.prepare_cached("SELECT request, device FROM joins WHERE uid = ?1 AND open = 1").db()?;
        let rows = stmt.query_map(params![uid.as_str()], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))).db()?;
        let mut out = Vec::new();
        for row in rows {
            let (request, device) = row.db()?;
            out.push((RequestId::parse(&request).map_err(internal)?, DeviceId::parse(&device).map_err(internal)?));
        }
        Ok(out)
    }

    fn info(&self, row: &JoinRow) -> Result<JoinRequestInfo, WireError> {
        let device = self.device(&row.device)?.ok_or_else(|| internal("批准请求的电脑不见了"))?;
        Ok(JoinRequestInfo {
            request: row.request.clone(),
            device: device.keys,
            name: device.name,
            created_ms: row.created_ms,
            expires_ms: row.expires_ms,
            attempt: row.attempt,
            pake: row.pake.clone(),
            proof: row.proof,
        })
    }

    /// 关掉一个请求，两边都收到“结束”。
    fn close_join(&self, row: &JoinRow, reason: JoinCloseReason, effects: &mut Effects) -> Result<(), WireError> {
        self.conn
            .execute("UPDATE joins SET open = 0 WHERE request = ?1", params![row.request.as_str()])
            .db()?;
        let push = Push::JoinClosed {
            request: row.request.clone(),
            reason,
        };
        effects.push(Target::Device(row.device.clone()), push.clone());
        effects.push(Target::Account(row.uid.clone()), push);
        tracing::info!(uid = %row.uid, ?reason, "批准请求结束");
        Ok(())
    }

    /// 取一个还有效的请求；过期了就当场关掉并报过期。
    fn live_join(&self, request: &RequestId, uid: &Uid, now: i64, effects: &mut Effects) -> Result<JoinRow, WireError> {
        let row = self
            .join_row(request)?
            .filter(|r| &r.uid == uid && r.open)
            .ok_or_else(|| fail(ErrorCode::NotFound, "批准请求不存在或已结束"))?;
        if row.expires_ms <= now {
            self.close_join(&row, JoinCloseReason::Expired, effects)?;
            return Err(fail(ErrorCode::Expired, "批准请求已过期"));
        }
        Ok(row)
    }

    /// 新电脑：发起批准请求。
    pub fn join_start(&mut self, s: &Session, pake: &Bytes, proof: &Fixed<64>, now: i64) -> OpResult {
        let row = self.require_pending(s)?;
        let account = self.account(&s.uid)?.ok_or_else(|| internal("找不到账号"))?;
        if !account.provisioned {
            return Err(fail(ErrorCode::Conflict, "账号还没开通"));
        }
        if pake.is_empty() || pake.len() > PAKE_MAX {
            return Err(fail(ErrorCode::BadRequest, "握手消息长度不对"));
        }
        // 先验证明：拿不出私钥的请求不打扰旧电脑（旧电脑签“加电脑”时链上还会再验一遍）。
        check_proof(&row.keys, proof, &s.uid, account.epoch, ProofRole::Device).map_err(|_| fail(ErrorCode::Invalid, "新电脑的证明不对"))?;
        let mut effects = Effects::default();
        for (request, device) in self.open_joins_of(&s.uid)? {
            let row = self.join_row(&request)?.ok_or_else(|| internal("请求不见了"))?;
            if row.expires_ms <= now {
                self.close_join(&row, JoinCloseReason::Expired, &mut effects)?;
            } else if device == s.device {
                // 同一台新电脑重新发起：旧的作废。
                self.close_join(&row, JoinCloseReason::Cancelled, &mut effects)?;
            } else {
                return Err(fail(ErrorCode::Conflict, "另一台电脑正在请求加入，请稍后再试"));
            }
        }
        let recent: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM joins WHERE uid = ?1 AND created_ms > ?2",
                params![s.uid.as_str(), now - 3_600_000],
                |r| r.get(0),
            )
            .db()?;
        if recent >= i64::from(limits::JOIN_PER_HOUR) {
            return Err(fail(ErrorCode::RateLimited, "这一小时请求加入的次数太多了"));
        }
        let request = RequestId::random();
        let expires_ms = now + limits::JOIN_TTL_MS;
        self.conn
            .execute(
                "INSERT INTO joins(request, uid, device, created_ms, expires_ms, attempt, pake, proof) VALUES(?1, ?2, ?3, ?4, ?5, 1, ?6, ?7)",
                params![request.as_str(), s.uid.as_str(), s.device.as_str(), now, expires_ms, pake.0.clone(), proof.0.to_vec()],
            )
            .db()?;
        let row = self.join_row(&request)?.ok_or_else(|| internal("刚写的请求不见了"))?;
        effects.push(Target::Account(s.uid.clone()), Push::JoinRequested { request: self.info(&row)? });
        tracing::info!(uid = %s.uid, "新电脑请求加入");
        Ok((
            Response::JoinStarted {
                request,
                attempt: 1,
                expires_ms,
            },
            effects,
        ))
    }

    /// 新电脑：上一次输错了，用新的第一步再试（计一次错）。
    pub fn join_retry(&mut self, s: &Session, request: &RequestId, pake: &Bytes, now: i64) -> OpResult {
        self.require_pending(s)?;
        let mut effects = Effects::default();
        let row = self.live_join(request, &s.uid, now, &mut effects)?;
        if row.device != s.device {
            return Err(fail(ErrorCode::Forbidden, "不是这台电脑的请求"));
        }
        if pake.is_empty() || pake.len() > PAKE_MAX {
            return Err(fail(ErrorCode::BadRequest, "握手消息长度不对"));
        }
        if row.attempt >= limits::JOIN_MAX_ATTEMPTS {
            self.close_join(&row, JoinCloseReason::TooManyAttempts, &mut effects)?;
            return Ok((
                Response::JoinStarted {
                    request: request.clone(),
                    attempt: row.attempt,
                    expires_ms: row.expires_ms,
                },
                effects,
            ));
        }
        let attempt = row.attempt + 1;
        self.conn
            .execute(
                "UPDATE joins SET attempt = ?1, pake = ?2, responder = NULL, answer = NULL WHERE request = ?3",
                params![attempt as i64, pake.0.clone(), request.as_str()],
            )
            .db()?;
        effects.push(
            Target::Account(s.uid.clone()),
            Push::JoinRetry {
                request: request.clone(),
                attempt,
                pake: pake.clone(),
            },
        );
        Ok((
            Response::JoinStarted {
                request: request.clone(),
                attempt,
                expires_ms: row.expires_ms,
            },
            effects,
        ))
    }

    /// 旧电脑：回应。
    pub fn join_respond(&mut self, s: &Session, request: &RequestId, attempt: u32, pake: &Bytes, confirm: &Fixed<32>, now: i64) -> OpResult {
        self.require_active(s)?;
        let mut effects = Effects::default();
        let row = self.live_join(request, &s.uid, now, &mut effects)?;
        if row.attempt != attempt {
            return Err(fail(ErrorCode::Conflict, "新电脑已经开始了新的一次尝试"));
        }
        if pake.is_empty() || pake.len() > PAKE_MAX {
            return Err(fail(ErrorCode::BadRequest, "握手消息长度不对"));
        }
        let answer = Answer {
            attempt,
            pake: pake.clone(),
            confirm: *confirm,
        };
        self.conn
            .execute(
                "UPDATE joins SET responder = ?1, answer = ?2 WHERE request = ?3",
                params![s.device.as_str(), to_json(&answer)?, request.as_str()],
            )
            .db()?;
        effects.push(
            Target::Device(row.device.clone()),
            Push::JoinAnswered {
                request: request.clone(),
                attempt,
                pake: pake.clone(),
                confirm: *confirm,
            },
        );
        Ok((Response::Done, effects))
    }

    /// 断线重连的新电脑：把已经收到的回应再推一次。
    pub(crate) fn replay_join_answer(&self, device: &DeviceId, now: i64, effects: &mut Effects) -> Result<(), WireError> {
        let request: Option<String> = self
            .conn
            .query_row(
                "SELECT request FROM joins WHERE device = ?1 AND open = 1 AND expires_ms > ?2 AND answer IS NOT NULL",
                params![device.as_str(), now],
                |r| r.get(0),
            )
            .optional()
            .db()?;
        if let Some(request) = request {
            let row = self.join_row(&RequestId::parse(&request).map_err(internal)?)?.ok_or_else(|| internal("请求不见了"))?;
            if let Some(answer) = row.answer {
                effects.push(
                    Target::Device(device.clone()),
                    Push::JoinAnswered {
                        request: row.request,
                        attempt: answer.attempt,
                        pake: answer.pake,
                        confirm: answer.confirm,
                    },
                );
            }
        }
        Ok(())
    }

    /// 新电脑：回确认码，转给回应的那台旧电脑。
    pub fn join_confirm(&mut self, s: &Session, request: &RequestId, attempt: u32, confirm: &Fixed<32>, now: i64) -> OpResult {
        self.require_pending(s)?;
        let mut effects = Effects::default();
        let row = self.live_join(request, &s.uid, now, &mut effects)?;
        if row.device != s.device || row.attempt != attempt {
            return Err(fail(ErrorCode::Conflict, "请求或尝试次数对不上"));
        }
        let responder = row.responder.clone().ok_or_else(|| fail(ErrorCode::Conflict, "旧电脑还没回应"))?;
        effects.push(
            Target::Device(responder),
            Push::JoinConfirmed {
                request: request.clone(),
                attempt,
                confirm: *confirm,
            },
        );
        Ok((Response::Done, effects))
    }

    /// 新电脑：不等了。
    pub fn join_cancel(&mut self, s: &Session, request: &RequestId, now: i64) -> OpResult {
        self.require_pending(s)?;
        let mut effects = Effects::default();
        let row = self.live_join(request, &s.uid, now, &mut effects)?;
        if row.device != s.device {
            return Err(fail(ErrorCode::Forbidden, "不是这台电脑的请求"));
        }
        self.close_join(&row, JoinCloseReason::Cancelled, &mut effects)?;
        Ok((Response::Done, effects))
    }

    /// 旧电脑：拒绝。
    pub fn join_reject(&mut self, s: &Session, request: &RequestId, now: i64) -> OpResult {
        self.require_active(s)?;
        let mut effects = Effects::default();
        let row = self.live_join(request, &s.uid, now, &mut effects)?;
        self.close_join(&row, JoinCloseReason::Rejected, &mut effects)?;
        Ok((Response::Done, effects))
    }

    /// 待处理的批准请求。
    pub fn list_join_requests(&mut self, s: &Session, now: i64) -> OpResult {
        self.require_active(s)?;
        let mut effects = Effects::default();
        let mut requests = Vec::new();
        for (request, _) in self.open_joins_of(&s.uid)? {
            let row = self.join_row(&request)?.ok_or_else(|| internal("请求不见了"))?;
            if row.expires_ms <= now {
                self.close_join(&row, JoinCloseReason::Expired, &mut effects)?;
            } else {
                requests.push(self.info(&row)?);
            }
        }
        Ok((Response::JoinRequests { requests }, effects))
    }

    /// 核对“加电脑”这一环并写入：新电脑变成已批准，收到账号钥匙当前一代。
    #[allow(clippy::too_many_arguments)] // 批准与恢复密钥加入共用的写入，参数都是这一次写入要的
    fn add_device(&mut self, uid: &Uid, device: &DeviceId, link: &AccountLink, keys: &AccountKeyBundle, recovery_only: bool, now: i64, effects: &mut Effects) -> Result<u64, WireError> {
        let account = self.account(uid)?.ok_or_else(|| internal("找不到账号"))?;
        let mut state = self.current_state(uid)?.ok_or_else(|| internal("账号没有签名链"))?;
        if state.active_devices().count() >= limits::MAX_DEVICES {
            return Err(fail(ErrorCode::LimitExceeded, "这个账号的电脑已经到上限了，先移除一台"));
        }
        let body = link.parse().map_err(|e| fail(ErrorCode::Invalid, e.to_string()))?;
        let AccountChange::AddDevice { device: entry, .. } = &body.change else {
            return Err(fail(ErrorCode::Invalid, "这一环应是加电脑"));
        };
        let row = self.device(device)?.ok_or_else(|| internal("新电脑不见了"))?;
        if entry.keys != row.keys {
            return Err(fail(ErrorCode::Invalid, "签名链里的新电脑不是请求加入的那台"));
        }
        if recovery_only && link.by != state.recovery.id() {
            return Err(fail(ErrorCode::Invalid, "这一环应由恢复密钥签"));
        }
        let log = self.last_log + 1;
        state.apply(link, log).map_err(|e| fail(ErrorCode::Invalid, e.to_string()))?;
        let expected: BTreeSet<DeviceId> = [device.clone()].into();
        Self::check_account_bundle(keys, state.current_key().generation, &expected, false)?;
        let recipients = self.contacts(uid)?;
        let tx = self.conn.unchecked_transaction().db()?;
        let event = self.write_account_link(&tx, uid, account.epoch, state.seq, link, now, &recipients)?;
        if event.log != log {
            return Err(internal("日志序号与预期不符"));
        }
        Self::write_account_bundle(&tx, uid, account.epoch, keys)?;
        tx.execute(
            "UPDATE devices SET status = 'active', joined_ms = ?1, name = ?2 WHERE device = ?3",
            params![now, entry.name, device.as_str()],
        )
        .db()?;
        tx.commit().db()?;
        self.last_log = log;
        self.accounts.entry(uid.clone()).or_default().epochs.insert(account.epoch, state);
        for r in &recipients {
            effects.push(Target::Account(r.clone()), Push::Event { event: event.clone() });
        }
        effects.approve.push(device.clone());
        Ok(log)
    }

    /// 旧电脑：确认码对上后批准。
    pub fn join_approve(&mut self, s: &Session, request: &RequestId, link: &AccountLink, keys: &AccountKeyBundle, now: i64) -> OpResult {
        self.require_active(s)?;
        let mut effects = Effects::default();
        let row = self.live_join(request, &s.uid, now, &mut effects)?;
        let log = self.add_device(&s.uid, &row.device, link, keys, false, now, &mut effects)?;
        self.close_join(&row, JoinCloseReason::Approved, &mut effects)?;
        tracing::info!(uid = %s.uid, log, "新电脑已批准");
        Ok((Response::Appended { log }, effects))
    }

    /// 新电脑：用恢复密钥加入。
    pub fn join_with_recovery(&mut self, s: &Session, link: &AccountLink, keys: &AccountKeyBundle, now: i64) -> OpResult {
        self.require_pending(s)?;
        let mut effects = Effects::default();
        let log = self.add_device(&s.uid, &s.device, link, keys, true, now, &mut effects)?;
        for (request, device) in self.open_joins_of(&s.uid)? {
            if device == s.device {
                let row = self.join_row(&request)?.ok_or_else(|| internal("请求不见了"))?;
                self.close_join(&row, JoinCloseReason::Cancelled, &mut effects)?;
            }
        }
        tracing::info!(uid = %s.uid, log, "新电脑用恢复密钥加入");
        Ok((Response::Appended { log }, effects))
    }

    /// 定时任务：作废过期的请求。
    pub fn expire_joins(&mut self, now: i64) -> Result<Effects, WireError> {
        let requests: Vec<String> = {
            let mut stmt = self.conn.prepare_cached("SELECT request FROM joins WHERE open = 1 AND expires_ms <= ?1").db()?;
            let rows = stmt.query_map(params![now], |r| r.get(0)).db()?;
            rows.collect::<Result<_, _>>().db()?
        };
        let mut effects = Effects::default();
        for request in requests {
            if let Some(row) = self.join_row(&RequestId::parse(&request).map_err(internal)?)? {
                self.close_join(&row, JoinCloseReason::Expired, &mut effects)?;
            }
        }
        Ok(effects)
    }
}

#[cfg(test)]
mod tests {
    use friend_im_core::account::{AccountLink, DeviceEntry, eldest_body};
    use friend_im_core::crypto;
    use friend_im_core::keys::{AccountSeed, DeviceSecret, RecoveryKey, account_seed_info};
    use friend_im_core::wire::{DeviceKeyBox, Push};

    use super::*;
    use crate::cindy::CindyIdentity;
    use crate::ops_account::LoginAuth;
    use crate::store::open_memory;

    const T0: i64 = 1_700_000_000_000;

    fn login(store: &mut Store, device: &DeviceSecret, name: &str) -> Session {
        let identity = CindyIdentity {
            issuer: "https://auth.cindy.app".into(),
            member: "alice".into(),
            name: "alice".into(),
        };
        store.login(LoginAuth::Cindy(identity), device.public(), name, T0).unwrap().0
    }

    /// 开通好的账号 + 一台待批准的新电脑。
    fn account_with_pending() -> (Store, Session, Session, DeviceSecret) {
        let mut store = open_memory();
        let first = DeviceSecret::generate();
        let s1 = login(&mut store, &first, "台式机");
        let recovery = RecoveryKey::generate().device_secret();
        let seed = AccountSeed::generate();
        let entry = DeviceEntry { keys: first.public(), name: "台式机".into() };
        let link = AccountLink::sign(&eldest_body(&s1.uid, 0, entry, &recovery, seed.public(1), T0), &first);
        let boxes = [first.public(), recovery.public()]
            .into_iter()
            .map(|to| DeviceKeyBox {
                device: to.id(),
                sealed: crypto::seal_to(&to.enc.0, &account_seed_info(&s1.uid, 0, 1, &to.id()), seed.as_bytes()).unwrap(),
            })
            .collect();
        store.provision(&s1, &link, &AccountKeyBundle { generation: 1, boxes, prev: None }, T0).unwrap();
        let second = DeviceSecret::generate();
        let s2 = login(&mut store, &second, "笔记本");
        (store, s1, s2, second)
    }

    fn start(store: &mut Store, s: &Session, device: &DeviceSecret, now: i64) -> Result<RequestId, WireError> {
        let proof = account_proof(device, &s.uid);
        match store.join_start(s, &Bytes(vec![1; 33]), &proof, now)?.0 {
            Response::JoinStarted { request, .. } => Ok(request),
            other => panic!("回执不对：{other:?}"),
        }
    }

    fn account_proof(device: &DeviceSecret, uid: &Uid) -> Fixed<64> {
        friend_im_core::account::device_proof(device, uid, 0, ProofRole::Device)
    }

    fn closed(effects: &Effects, reason: JoinCloseReason) -> Vec<Target> {
        effects
            .pushes
            .iter()
            .filter(|(_, p)| matches!(p, Push::JoinClosed { reason: r, .. } if *r == reason))
            .map(|(t, _)| t.clone())
            .collect()
    }

    #[test]
    fn requests_expire_after_ten_minutes_and_both_sides_hear_it() {
        let (mut store, s1, s2, second) = account_with_pending();
        let request = start(&mut store, &s2, &second, T0).unwrap();
        // 9 分 59 秒：还在，旧电脑列得出来。
        assert!(store.expire_joins(T0 + limits::JOIN_TTL_MS - 1).unwrap().pushes.is_empty());
        let (Response::JoinRequests { requests }, _) = store.list_join_requests(&s1, T0 + limits::JOIN_TTL_MS - 1).unwrap() else { panic!() };
        assert_eq!(requests.len(), 1);
        // 满 10 分钟：作废，新电脑和旧电脑都收到“结束”。
        let effects = store.expire_joins(T0 + limits::JOIN_TTL_MS).unwrap();
        assert_eq!(closed(&effects, JoinCloseReason::Expired), [Target::Device(s2.device.clone()), Target::Account(s1.uid.clone())]);
        // 作废后旧电脑再回应、列请求，都没有了。
        let err = store.join_respond(&s1, &request, 1, &Bytes(vec![1; 33]), &Fixed([0; 32]), T0 + limits::JOIN_TTL_MS).unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        let (Response::JoinRequests { requests }, _) = store.list_join_requests(&s1, T0 + limits::JOIN_TTL_MS).unwrap() else { panic!() };
        assert!(requests.is_empty());
        // 新电脑可以重新请求。
        start(&mut store, &s2, &second, T0 + limits::JOIN_TTL_MS + 1).unwrap();
    }

    #[test]
    fn an_expired_request_is_closed_when_touched_even_before_the_timer_runs() {
        let (mut store, s1, s2, second) = account_with_pending();
        let request = start(&mut store, &s2, &second, T0).unwrap();
        let later = T0 + limits::JOIN_TTL_MS + 5;
        let err = store.join_respond(&s1, &request, 1, &Bytes(vec![1; 33]), &Fixed([0; 32]), later).unwrap_err();
        assert_eq!(err.code, ErrorCode::Expired);
        assert!(store.expire_joins(later).unwrap().pushes.is_empty(), "已经关过，定时任务不再重复通知");
    }

    #[test]
    fn at_most_ten_requests_per_hour() {
        let (mut store, _, s2, second) = account_with_pending();
        // 同一台新电脑重新发起会作废上一个，但都算进这一小时的次数。
        for i in 0..limits::JOIN_PER_HOUR as i64 {
            start(&mut store, &s2, &second, T0 + i).unwrap();
        }
        let err = start(&mut store, &s2, &second, T0 + 100).unwrap_err();
        assert_eq!(err.code, ErrorCode::RateLimited);
        // 一小时后又可以。
        start(&mut store, &s2, &second, T0 + 3_600_001).unwrap();
    }

    #[test]
    fn a_proof_for_another_account_is_rejected_before_bothering_old_computers() {
        let (mut store, _, s2, second) = account_with_pending();
        let wrong = friend_im_core::account::device_proof(&second, &Uid::random(), 0, ProofRole::Device);
        let err = store.join_start(&s2, &Bytes(vec![1; 33]), &wrong, T0).unwrap_err();
        assert_eq!(err.code, ErrorCode::Invalid);
    }
}
