//! 转交、在线设备与控制面补拉。服务器在这里什么都不存：帧只在内存里过一下，接收方不在线就回“不在线”。

use std::collections::{BTreeSet, VecDeque};

use friend_im_core::ids::{ConvId, DeviceId, Uid};
use friend_im_core::limits;
use friend_im_core::peer::SealedFrame;
use friend_im_core::wire::{ErrorCode, PeerInfo, Response, SyncEvent, WireError};
use rusqlite::{OptionalExtension, params};

use crate::ops_account::{OpResult, Session};
use crate::store::{DbResult, DeviceState, Effects, Store, fail, from_json, internal};

/// 发送频率记录：每个账号最近一分钟的发送时间。
pub struct RateLimiter {
    per_second: usize,
    per_minute: usize,
    sends: std::collections::HashMap<Uid, VecDeque<i64>>,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(limits::RELAY_PER_SECOND, limits::RELAY_PER_MINUTE)
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
        let last_second = times.iter().rev().take_while(|t| **t > now - 1_000).count();
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

impl Store {
    /// 转交前核对：发送设备已批准、帧确实是它发的、接收设备属于同一账号或同一会话的当前成员。
    pub fn relay_check(&mut self, s: &Session, to: &DeviceId, conv: Option<&ConvId>, frame: &SealedFrame, limiter: &mut RateLimiter, now: i64) -> Result<(), WireError> {
        self.require_active(s)?;
        if frame.from != s.device || frame.uid != s.uid || frame.to != *to {
            return Err(fail(ErrorCode::Forbidden, "只能用自己这台电脑发帧"));
        }
        let owner: Option<(String, String)> = self
            .conn
            .query_row("SELECT uid, status FROM devices WHERE device = ?1", params![to.as_str()], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()
            .db()?;
        let Some((owner, status)) = owner else {
            return Err(fail(ErrorCode::NotFound, "没有这台电脑"));
        };
        if DeviceState::parse(&status) != DeviceState::Active {
            return Err(fail(ErrorCode::Forbidden, "这台电脑还不能收东西"));
        }
        let owner = Uid::parse(&owner).map_err(internal)?;
        if owner != s.uid {
            let conv = conv.ok_or_else(|| fail(ErrorCode::Forbidden, "发给别的账号要说明是哪个会话"))?;
            if self.membership(conv, &s.uid)? != Some(None) || self.membership(conv, &owner)? != Some(None) {
                return Err(fail(ErrorCode::Forbidden, "不是这个会话现在的成员"));
            }
        }
        if !limiter.allow(&s.uid, now) {
            return Err(fail(ErrorCode::RateLimited, "转交太频繁，请稍后再试"));
        }
        Ok(())
    }

    /// 发消息之前的核对：还是成员、身份没变、不用先换代、钥匙代数是最新的。只答这一句，不经手内容，也不记。
    pub fn before_send(&mut self, s: &Session, conv: &ConvId, generation: u32) -> OpResult {
        self.require_active(s)?;
        if self.membership(conv, &s.uid)? != Some(None) {
            return Err(fail(ErrorCode::Forbidden, "不是这个会话现在的成员"));
        }
        let state = self.conv_state(conv)?.ok_or_else(|| fail(ErrorCode::NotFound, "没有这个会话"))?;
        let rotation_needed: i64 = self
            .conn
            .query_row("SELECT rotation_needed FROM convs WHERE conv = ?1", params![conv.as_str()], |r| r.get(0))
            .db()?;
        if rotation_needed != 0 || state.rotation_pending {
            return Err(fail(ErrorCode::RotationRequired, "这个会话要先换代"));
        }
        if generation != state.current_key().generation {
            // 发送方看到的会话签名链落后了（别人刚换过代）：刷新后用新一代重新加密。
            return Err(fail(ErrorCode::Conflict, "会话钥匙已换代，请刷新后重发"));
        }
        Ok((Response::Done, Effects::default()))
    }

    /// 和这个账号共享会话的账号（都是当前成员），含它自己。
    pub fn peers_of(&self, uid: &Uid) -> Result<BTreeSet<Uid>, WireError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT DISTINCT b.uid FROM conv_members a JOIN conv_members b ON b.conv = a.conv \
                 WHERE a.uid = ?1 AND a.left_log IS NULL AND b.left_log IS NULL",
            )
            .db()?;
        let rows = stmt.query_map(params![uid.as_str()], |r| r.get::<_, String>(0)).db()?;
        let mut peers = BTreeSet::from([uid.clone()]);
        for row in rows {
            peers.insert(Uid::parse(&row.db()?).map_err(internal)?);
        }
        Ok(peers)
    }

    /// 现在在线、能和这台设备对账的设备。
    pub fn online_peers(&self, s: &Session, online: &dyn Fn(&Uid) -> Vec<DeviceId>) -> OpResult {
        self.require_active(s)?;
        let mut peers = Vec::new();
        for uid in self.peers_of(&s.uid)? {
            for device in online(&uid) {
                if device != s.device {
                    peers.push(PeerInfo { uid: uid.clone(), device });
                }
            }
        }
        Ok((Response::Peers { peers }, Effects::default()))
    }

    /// 补拉序号大于 `since` 的控制面事件。
    pub fn sync(&mut self, s: &Session, since: u64, limit: u32) -> OpResult {
        self.require_active(s)?;
        let limit = limit.clamp(1, limits::SYNC_PAGE_MAX);
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT f.seq, l.ts_ms, l.payload FROM feed f JOIN log l ON l.seq = f.seq \
                 WHERE f.uid = ?1 AND f.seq > ?2 ORDER BY f.seq LIMIT ?3",
            )
            .db()?;
        let rows = stmt
            .query_map(params![s.uid.as_str(), since as i64, limit as i64 + 1], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, Option<String>>(2)?))
            })
            .db()?;
        let mut events = Vec::new();
        let mut bytes = 0;
        let mut more = false;
        for row in rows {
            let (seq, ts_ms, payload) = row.db()?;
            let payload = payload.ok_or_else(|| internal("日志缺内容"))?;
            if events.len() == limit as usize || (!events.is_empty() && bytes + payload.len() > limits::RESPONSE_SOFT_MAX_BYTES) {
                more = true;
                break;
            }
            bytes += payload.len();
            events.push(SyncEvent { log: seq as u64, ts_ms, event: from_json(&payload)? });
        }
        let next = events.last().map(|e| e.log).unwrap_or(since);
        Ok((Response::SyncBatch { events, next, more }, Effects::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiter_enforces_per_second_and_per_minute() {
        let mut limiter = RateLimiter::new(5, 120);
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
