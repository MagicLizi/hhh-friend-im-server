//! 会话：建私聊 / 群、接会话签名链、钥匙封装、群邀请。
//!
//! 服务器按 core 的规则重新验每一环（插件也会验，服务器验是为了不让有问题的客户端把共享数据写坏），
//! 再按批量规则检查：
//!
//! - 群主移出别人必须同一个请求里换代；
//! - 换代必须封装给全部成员、且封装给他们账号钥匙的当前一代；换代后的成员表里不能有重置过账号还没改记的人
//!   （群里先移出，私聊先改记新身份），新钥匙不会落到只凭 Cindy 账号重置的人手里；
//! - 对方重置后改记新身份的那一代不带旧钥匙链：重置的人看不到重置前的消息（契约 S7），
//!   掌握 Cindy 账号的人重置别人的账号也读不到历史。

use std::collections::BTreeSet;

use friend_im_core::conv::{ConvChange, ConvKind, ConvLink, ConvState, RemoveReason, link_uids};
use friend_im_core::ids::{ConvId, InviteId, Uid};
use friend_im_core::limits;
use friend_im_core::wire::{ConvEntry, ConvGenerationBox, ConvKeyBundle, ConvSummary, ErrorCode, Event, GenerationWrap, InviteSeed, Push, Response, SyncEvent, WireError};
use rusqlite::{OptionalExtension, params};

use crate::ops_account::{OpResult, Session};
use crate::store::{CacheDirectory, DbResult, Effects, Store, Target, fail, from_json, internal, to_json};

/// 一个请求里各类变化的统计，用来套批量规则、更新成员区间。
#[derive(Default)]
struct BatchChanges {
    added: Vec<(Uid, usize)>,
    removed: Vec<(Uid, usize)>,
    kicked: bool,
    member_reset: bool,
    invites: Vec<InviteId>,
}

fn kind_str(kind: ConvKind) -> &'static str {
    match kind {
        ConvKind::Dm => "dm",
        ConvKind::Group => "group",
    }
}

fn dm_key(a: &Uid, b: &Uid) -> String {
    if a < b { format!("{a}:{b}") } else { format!("{b}:{a}") }
}

impl Store {
    /// 核对一份会话钥匙封装：代数对、收件人正好是期望的成员、每人一份、封装给他们账号钥匙的当前一代。
    fn check_conv_bundle(&mut self, bundle: &ConvKeyBundle, generation: u32, expected: &BTreeSet<Uid>, need_prev: Option<bool>) -> Result<(), WireError> {
        if bundle.generation != generation {
            return Err(fail(ErrorCode::Invalid, "会话钥匙封装的代数不对"));
        }
        let got: BTreeSet<&Uid> = bundle.boxes.iter().map(|b| &b.uid).collect();
        if got.len() != bundle.boxes.len() || got != expected.iter().collect() {
            return Err(fail(ErrorCode::Invalid, "会话钥匙封装的收件人不对"));
        }
        for b in &bundle.boxes {
            if b.sealed.ct.len() > 128 {
                return Err(fail(ErrorCode::Invalid, "会话钥匙封装太大"));
            }
            let account = self.account(&b.uid)?.ok_or_else(|| fail(ErrorCode::Invalid, "封装的收件人不存在"))?;
            let state = self.current_state(&b.uid)?.ok_or_else(|| fail(ErrorCode::Invalid, "封装的收件人还没开通"))?;
            if b.account_epoch != account.epoch || b.account_generation != state.current_key().generation {
                return Err(fail(ErrorCode::RotationRequired, "封装没有用收件人账号钥匙的当前一代，请刷新后重试"));
            }
        }
        match (need_prev, &bundle.prev) {
            (Some(true), None) | (Some(false), Some(_)) => return Err(fail(ErrorCode::Invalid, "旧钥匙链不对")),
            (_, Some(prev)) if prev.len() > 128 => return Err(fail(ErrorCode::Invalid, "旧钥匙链太大")),
            _ => {}
        }
        Ok(())
    }

    fn write_conv_bundle(conn: &rusqlite::Connection, conv: &ConvId, bundle: &ConvKeyBundle) -> Result<(), WireError> {
        for b in &bundle.boxes {
            conn.execute(
                "INSERT OR REPLACE INTO conv_keys(conv, generation, uid, account_epoch, account_generation, sealed) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    conv.as_str(),
                    bundle.generation as i64,
                    b.uid.as_str(),
                    b.account_epoch as i64,
                    b.account_generation as i64,
                    to_json(&b.sealed)?
                ],
            )
            .db()?;
        }
        if let Some(prev) = &bundle.prev {
            conn.execute(
                "INSERT OR REPLACE INTO conv_prev(conv, generation, wrapped) VALUES(?1, ?2, ?3)",
                params![conv.as_str(), bundle.generation as i64, prev.0.clone()],
            )
            .db()?;
        }
        Ok(())
    }

    fn write_conv_link(conn: &rusqlite::Connection, conv: &ConvId, seq: u64, link: &ConvLink, now: i64, recipients: &BTreeSet<Uid>) -> Result<SyncEvent, WireError> {
        let event = Event::ConvLink {
            conv: conv.clone(),
            link: link.clone(),
        };
        let log = Self::append_log(conn, now, "conv_link", Some(&to_json(&event)?))?;
        conn.execute(
            "INSERT INTO conv_links(conv, seq, link, log) VALUES(?1, ?2, ?3, ?4)",
            params![conv.as_str(), seq as i64, to_json(link)?, log as i64],
        )
        .db()?;
        Self::fan_out(conn, log, recipients)?;
        Ok(SyncEvent { log, ts_ms: now, event })
    }

    fn write_keys_event(conn: &rusqlite::Connection, conv: &ConvId, bundle: &ConvKeyBundle, now: i64) -> Result<(SyncEvent, Vec<Uid>), WireError> {
        let event = Event::ConvKeys {
            conv: conv.clone(),
            generation: bundle.generation,
        };
        let log = Self::append_log(conn, now, "conv_keys", Some(&to_json(&event)?))?;
        let uids: Vec<Uid> = bundle.boxes.iter().map(|b| b.uid.clone()).collect();
        Self::fan_out(conn, log, &uids)?;
        Ok((SyncEvent { log, ts_ms: now, event }, uids))
    }

    /// 签名人必须是这条连接的账号和电脑，身份指纹必须是当前这一轮。
    fn check_author(&mut self, s: &Session, link: &ConvLink) -> Result<friend_im_core::conv::ConvLinkBody, WireError> {
        let body = link.parse().map_err(|e| fail(ErrorCode::Invalid, e.to_string()))?;
        if body.author != s.uid || link.by != s.device {
            return Err(fail(ErrorCode::Forbidden, "只能提交自己这台电脑签的环"));
        }
        let eldest = self.current_state(&s.uid)?.map(|st| st.eldest);
        if eldest != Some(body.author_eldest) {
            return Err(fail(ErrorCode::Invalid, "签名的身份指纹不是当前这一轮"));
        }
        Ok(body)
    }

    /// 建会话。
    pub fn create_conv(&mut self, s: &Session, link: &ConvLink, keys: &ConvKeyBundle, now: i64) -> OpResult {
        self.require_active(s)?;
        let body = self.check_author(s, link)?;
        let ConvChange::Create { kind, members, .. } = &body.change else {
            return Err(fail(ErrorCode::Invalid, "第一环必须是建会话"));
        };
        let conv = body.conv.clone();
        let exists: Option<i64> = self.conn.query_row("SELECT 1 FROM convs WHERE conv = ?1", params![conv.as_str()], |r| r.get(0)).optional().db()?;
        if exists.is_some() {
            return Err(fail(ErrorCode::Conflict, "会话编号已存在"));
        }
        for m in members {
            let eldest = self.current_state(&m.uid)?.map(|st| st.eldest);
            if eldest != Some(m.eldest) {
                return Err(fail(ErrorCode::Invalid, "成员不存在、没开通或身份指纹已变"));
            }
        }
        let uids: Vec<Uid> = members.iter().map(|m| m.uid.clone()).collect();
        self.preload_accounts(&uids)?;
        let log = self.last_log + 1;
        let state = ConvState::create(&conv, link, log, &CacheDirectory { accounts: &self.accounts }).map_err(|e| fail(ErrorCode::Invalid, e.to_string()))?;
        let dm = (*kind == ConvKind::Dm).then(|| dm_key(&uids[0], &uids[1]));
        if let Some(key) = &dm {
            let existing: Option<String> = self.conn.query_row("SELECT conv FROM convs WHERE dm_key = ?1", params![key], |r| r.get(0)).optional().db()?;
            if let Some(existing) = existing {
                // 私聊已经存在（比如两人同时加了对方）：把已有会话编号告诉客户端，直接用它。
                return Err(fail(ErrorCode::Conflict, format!("existing:{existing}")));
            }
        }
        let expected: BTreeSet<Uid> = uids.iter().cloned().collect();
        self.check_conv_bundle(keys, 1, &expected, Some(false))?;
        let tx = self.conn.unchecked_transaction().db()?;
        let event = Self::write_conv_link(&tx, &conv, 1, link, now, &expected)?;
        if event.log != log {
            return Err(internal("日志序号与预期不符"));
        }
        tx.execute(
            "INSERT INTO convs(conv, kind, dm_key, created_ms) VALUES(?1, ?2, ?3, ?4)",
            params![conv.as_str(), kind_str(*kind), dm, now],
        )
        .db()?;
        for uid in &uids {
            tx.execute("INSERT INTO conv_members(conv, uid, joined_log) VALUES(?1, ?2, ?3)", params![conv.as_str(), uid.as_str(), log as i64])
                .db()?;
        }
        Self::write_conv_bundle(&tx, &conv, keys)?;
        tx.commit().db()?;
        self.last_log = log;
        self.convs.insert(conv.clone(), state);
        tracing::info!(conv = %conv, kind = kind_str(*kind), members = uids.len(), "会话已建");
        let mut effects = Effects::default();
        for uid in &uids {
            effects.push(Target::Account(uid.clone()), Push::Event { event: event.clone() });
        }
        Ok((Response::ConvCreated { conv, log }, effects))
    }

    /// 接会话签名链。
    pub fn append_conv(&mut self, s: &Session, conv: &ConvId, links: &[ConvLink], keys: Option<&ConvKeyBundle>, invite: Option<&InviteSeed>, now: i64) -> OpResult {
        self.require_active(s)?;
        if links.is_empty() || links.len() > limits::CONV_LINKS_PER_REQUEST {
            return Err(fail(ErrorCode::BadRequest, "一次接的环数不对"));
        }
        if self.membership(conv, &s.uid)? != Some(None) {
            return Err(fail(ErrorCode::Forbidden, "不是这个会话现在的成员"));
        }
        let mut state = self.conv_state(conv)?.ok_or_else(|| fail(ErrorCode::NotFound, "没有这个会话"))?;
        let mut uids = link_uids(links);
        uids.extend(state.members.keys().cloned());
        self.preload_accounts(&uids)?;
        let generation_before = state.current_key().generation;
        let mut changes = BatchChanges::default();
        for (i, link) in links.iter().enumerate() {
            let body = self.check_author(s, link)?;
            match &body.change {
                ConvChange::Create { .. } | ConvChange::JoinByInvite { .. } => return Err(fail(ErrorCode::BadRequest, "这类变化走专门的请求")),
                ConvChange::Add { members } => changes.added.extend(members.iter().map(|m| (m.uid.clone(), i))),
                ConvChange::Remove { uid, reason } => {
                    changes.removed.push((uid.clone(), i));
                    changes.kicked |= *reason == RemoveReason::Kicked;
                }
                ConvChange::Leave => changes.removed.push((body.author.clone(), i)),
                ConvChange::MemberReset { .. } => changes.member_reset = true,
                ConvChange::CreateInvite { invite, .. } => changes.invites.push(invite.clone()),
                _ => {}
            }
            if let ConvChange::Add { members } = &body.change {
                for m in members {
                    let eldest = self.current_state(&m.uid)?.map(|st| st.eldest);
                    if eldest != Some(m.eldest) {
                        return Err(fail(ErrorCode::Invalid, "邀请的人不存在、没开通或身份指纹已变"));
                    }
                }
            }
            state
                .apply(link, self.last_log + 1 + i as u64, &CacheDirectory { accounts: &self.accounts })
                .map_err(|e| fail(ErrorCode::Invalid, e.to_string()))?;
        }
        if changes.kicked && state.rotation_pending {
            return Err(fail(ErrorCode::Invalid, "群主移出成员必须在同一个请求里换代"));
        }
        let generation_after = state.current_key().generation;
        let members_after: BTreeSet<Uid> = state.members.keys().cloned().collect();
        let added: BTreeSet<Uid> = changes.added.iter().map(|(u, _)| u.clone()).filter(|u| members_after.contains(u)).collect();
        match (generation_after - generation_before, keys) {
            (0, None) if added.is_empty() => {}
            (0, Some(bundle)) if !added.is_empty() => self.check_conv_bundle(bundle, generation_after, &added, Some(false))?,
            (1, Some(bundle)) => {
                for (uid, member) in &state.members {
                    let current = self.current_state(uid)?.map(|st| st.eldest);
                    if current != Some(member.eldest) {
                        return Err(fail(ErrorCode::RotationRequired, format!("成员 {uid} 重置了账号：群里先移出，私聊先改记新身份")));
                    }
                }
                let need_prev = Some(!changes.member_reset);
                self.check_conv_bundle(bundle, generation_after, &members_after, need_prev)?;
            }
            (0, _) => return Err(fail(ErrorCode::Invalid, "邀请成员要带封装给新成员的会话钥匙，没邀请就不要带")),
            (1, None) => return Err(fail(ErrorCode::Invalid, "换代必须带新一代的封装")),
            _ => return Err(fail(ErrorCode::BadRequest, "一个请求只能换一代")),
        }
        match (changes.invites.as_slice(), invite) {
            ([], None) => {}
            ([id], Some(seed)) if &seed.invite == id && seed.generation == generation_after && seed.seed.len() <= 128 => {}
            _ => return Err(fail(ErrorCode::Invalid, "群邀请与包起来的会话种子对不上")),
        }
        // 换代标记：成员换过账号钥匙或重置后服务器标的“要换代”，只有这一批真的换了代才清掉。
        let flagged: i64 = self
            .conn
            .query_row("SELECT rotation_needed FROM convs WHERE conv = ?1", params![conv.as_str()], |r| r.get(0))
            .db()?;
        let rotation_needed = state.rotation_pending || (flagged != 0 && generation_after == generation_before);
        // 每一环推给“这一环之前和之后的成员”，按库里的成员记录逐环算：被这一环移出的人收得到这一环、收不到之后的；
        // 已经按库里记录离开的人（重置后自动退群）收不到群里之后的任何变化。
        let mut current: BTreeSet<Uid> = self.current_members(conv)?.into_iter().collect();
        let first_seq = state.seq + 1 - links.len() as u64;
        let tx = self.conn.unchecked_transaction().db()?;
        let mut events = Vec::new();
        for (i, link) in links.iter().enumerate() {
            let mut recipients = current.clone();
            for (uid, _) in changes.added.iter().filter(|(_, at)| *at == i) {
                current.insert(uid.clone());
            }
            for (uid, _) in changes.removed.iter().filter(|(_, at)| *at == i) {
                current.remove(uid);
            }
            recipients.extend(current.iter().cloned());
            let event = Self::write_conv_link(&tx, conv, first_seq + i as u64, link, now, &recipients)?;
            for (uid, _) in changes.added.iter().filter(|(_, at)| *at == i) {
                tx.execute(
                    "INSERT OR IGNORE INTO conv_members(conv, uid, joined_log) VALUES(?1, ?2, ?3)",
                    params![conv.as_str(), uid.as_str(), event.log as i64],
                )
                .db()?;
            }
            for (uid, _) in changes.removed.iter().filter(|(_, at)| *at == i) {
                tx.execute(
                    "UPDATE conv_members SET left_log = ?1 WHERE conv = ?2 AND uid = ?3 AND left_log IS NULL",
                    params![event.log as i64, conv.as_str(), uid.as_str()],
                )
                .db()?;
            }
            events.push((event, recipients.iter().cloned().collect::<Vec<_>>()));
        }
        if let Some(bundle) = keys {
            Self::write_conv_bundle(&tx, conv, bundle)?;
            events.push(Self::write_keys_event(&tx, conv, bundle, now)?);
        }
        if let Some(seed) = invite {
            let expires_ms = links
                .iter()
                .filter_map(|l| l.parse().ok())
                .find_map(|b| match b.change {
                    ConvChange::CreateInvite { expires_ms, .. } => Some(expires_ms),
                    _ => None,
                })
                .unwrap_or(now + limits::INVITE_TTL_MS)
                .min(now + limits::INVITE_TTL_MS);
            tx.execute(
                "INSERT INTO invites(invite, conv, generation, seed, expires_ms) VALUES(?1, ?2, ?3, ?4, ?5)",
                params![seed.invite.as_str(), conv.as_str(), seed.generation as i64, seed.seed.0.clone(), expires_ms],
            )
            .db()?;
        }
        tx.execute(
            "UPDATE convs SET rotation_needed = ?1 WHERE conv = ?2",
            params![i64::from(rotation_needed), conv.as_str()],
        )
        .db()?;
        tx.commit().db()?;
        let last = events.last().map(|(e, _)| e.log).unwrap_or(self.last_log);
        self.last_log = last;
        self.convs.insert(conv.clone(), state);
        tracing::info!(conv = %conv, links = links.len(), rotated = generation_after > generation_before, "会话签名链已接");
        let mut effects = Effects::default();
        for (event, uids) in events {
            for uid in uids {
                effects.push(Target::Account(uid), Push::Event { event: event.clone() });
            }
        }
        Ok((Response::Appended { log: last }, effects))
    }

    /// 给还没有当前一代封装的成员补发。
    pub fn grant_conv_keys(&mut self, s: &Session, conv: &ConvId, keys: &ConvKeyBundle, now: i64) -> OpResult {
        self.require_active(s)?;
        if self.membership(conv, &s.uid)? != Some(None) {
            return Err(fail(ErrorCode::Forbidden, "不是这个会话现在的成员"));
        }
        let state = self.conv_state(conv)?.ok_or_else(|| fail(ErrorCode::NotFound, "没有这个会话"))?;
        let generation = state.current_key().generation;
        let have: BTreeSet<Uid> = {
            let mut stmt = self.conn.prepare_cached("SELECT uid FROM conv_keys WHERE conv = ?1 AND generation = ?2").db()?;
            let rows = stmt.query_map(params![conv.as_str(), generation as i64], |r| r.get::<_, String>(0)).db()?;
            let mut out = BTreeSet::new();
            for row in rows {
                out.insert(Uid::parse(&row.db()?).map_err(internal)?);
            }
            out
        };
        let wanted: BTreeSet<Uid> = keys.boxes.iter().map(|b| b.uid.clone()).collect();
        if wanted.is_empty() || wanted.iter().any(|u| !state.members.contains_key(u) || have.contains(u)) {
            return Err(fail(ErrorCode::Invalid, "只能补发给还没有当前一代封装的成员"));
        }
        self.check_conv_bundle(keys, generation, &wanted, Some(false))?;
        let tx = self.conn.unchecked_transaction().db()?;
        Self::write_conv_bundle(&tx, conv, keys)?;
        let (event, uids) = Self::write_keys_event(&tx, conv, keys, now)?;
        tx.commit().db()?;
        self.last_log = event.log;
        let mut effects = Effects::default();
        for uid in uids {
            effects.push(Target::Account(uid), Push::Event { event: event.clone() });
        }
        Ok((Response::Appended { log: event.log }, effects))
    }

    fn conv_entries(&self, conv: &ConvId, after: u64, cap_log: Option<u64>) -> Result<Vec<ConvEntry>, WireError> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT link, log FROM conv_links WHERE conv = ?1 AND seq > ?2 AND log <= ?3 ORDER BY seq")
            .db()?;
        let cap = cap_log.map(|c| c as i64).unwrap_or(i64::MAX);
        let rows = stmt.query_map(params![conv.as_str(), after as i64, cap], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))).db()?;
        let mut out = Vec::new();
        for row in rows {
            let (link, log) = row.db()?;
            out.push(ConvEntry {
                log: log as u64,
                link: from_json(&link)?,
            });
        }
        Ok(out)
    }

    /// 取会话签名链（离开过的人只能看到离开为止）。
    pub fn get_conv(&mut self, s: &Session, conv: &ConvId, after: u64) -> OpResult {
        self.require_active(s)?;
        let membership = self.membership(conv, &s.uid)?.ok_or_else(|| fail(ErrorCode::Forbidden, "不是这个会话的成员"))?;
        let entries = self.conv_entries(conv, after, membership)?;
        Ok((Response::ConvChain { conv: conv.clone(), entries }, Effects::default()))
    }

    /// 取封装给我的会话钥匙和旧钥匙链。
    pub fn get_conv_keys(&mut self, s: &Session, conv: &ConvId) -> OpResult {
        self.require_active(s)?;
        self.membership(conv, &s.uid)?.ok_or_else(|| fail(ErrorCode::Forbidden, "不是这个会话的成员"))?;
        let boxes: Vec<ConvGenerationBox> = {
            let mut stmt = self
                .conn
                .prepare_cached("SELECT generation, account_epoch, account_generation, sealed FROM conv_keys WHERE conv = ?1 AND uid = ?2 ORDER BY generation")
                .db()?;
            let rows = stmt
                .query_map(params![conv.as_str(), s.uid.as_str()], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?, r.get::<_, String>(3)?))
                })
                .db()?;
            let mut out = Vec::new();
            for row in rows {
                let (generation, account_epoch, account_generation, sealed) = row.db()?;
                out.push(ConvGenerationBox {
                    generation: generation as u32,
                    account_epoch: account_epoch as u32,
                    account_generation: account_generation as u32,
                    sealed: from_json(&sealed)?,
                });
            }
            out
        };
        let max_generation = boxes.iter().map(|b| b.generation).max().unwrap_or(0);
        let prev: Vec<GenerationWrap> = {
            let mut stmt = self
                .conn
                .prepare_cached("SELECT generation, wrapped FROM conv_prev WHERE conv = ?1 AND generation <= ?2 ORDER BY generation")
                .db()?;
            let rows = stmt
                .query_map(params![conv.as_str(), max_generation as i64], |r| {
                    Ok(GenerationWrap {
                        generation: r.get::<_, i64>(0)? as u32,
                        wrapped: friend_im_core::bytes::Bytes(r.get(1)?),
                    })
                })
                .db()?;
            rows.collect::<Result<_, _>>().db()?
        };
        Ok((Response::ConvKeys { conv: conv.clone(), boxes, prev }, Effects::default()))
    }

    /// 我所在（和离开过）的会话。
    pub fn list_convs(&mut self, s: &Session) -> OpResult {
        self.require_active(s)?;
        let rows: Vec<(String, String, i64, i64, i64)> = {
            let mut stmt = self
                .conn
                .prepare_cached(
                    "SELECT c.conv, c.kind, c.last_log, (SELECT MAX(seq) FROM conv_links l WHERE l.conv = c.conv), \
                     EXISTS(SELECT 1 FROM conv_members m2 WHERE m2.conv = c.conv AND m2.uid = ?1 AND m2.left_log IS NULL) \
                     FROM convs c WHERE c.conv IN (SELECT conv FROM conv_members WHERE uid = ?1) ORDER BY c.last_log DESC",
                )
                .db()?;
            let rows = stmt
                .query_map(params![s.uid.as_str()], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))
                .db()?;
            rows.collect::<Result<_, _>>().db()?
        };
        let convs = rows
            .into_iter()
            .map(|(conv, kind, last_log, chain_seq, member)| {
                Ok(ConvSummary {
                    conv: ConvId::parse(&conv).map_err(internal)?,
                    kind: if kind == "dm" { ConvKind::Dm } else { ConvKind::Group },
                    chain_seq: chain_seq as u64,
                    last_log: last_log as u64,
                    member: member != 0,
                })
            })
            .collect::<Result<_, WireError>>()?;
        Ok((Response::Convs { convs }, Effects::default()))
    }

    /// 按邀请编号取群邀请。
    pub fn get_invite(&mut self, s: &Session, invite: &InviteId, now: i64) -> OpResult {
        self.require_active(s)?;
        let row: Option<(String, i64, Vec<u8>, i64)> = self
            .conn
            .query_row("SELECT conv, generation, seed, expires_ms FROM invites WHERE invite = ?1", params![invite.as_str()], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .optional()
            .db()?;
        let (conv, generation, seed, expires_ms) = row.ok_or_else(|| fail(ErrorCode::NotFound, "群邀请无效或已失效"))?;
        if expires_ms <= now {
            return Err(fail(ErrorCode::Expired, "群邀请已过期"));
        }
        let conv = ConvId::parse(&conv).map_err(internal)?;
        let state = self.conv_state(&conv)?.ok_or_else(|| internal("群邀请对应的会话不见了"))?;
        if state.invites.get(invite).is_none_or(|i| i.revoked) {
            return Err(fail(ErrorCode::NotFound, "群邀请无效或已失效"));
        }
        let entries = self.conv_entries(&conv, 0, None)?;
        Ok((
            Response::Invite {
                conv,
                generation: generation as u32,
                seed: friend_im_core::bytes::Bytes(seed),
                entries,
            },
            Effects::default(),
        ))
    }

    /// 凭群邀请链接加入。
    pub fn join_by_invite(&mut self, s: &Session, conv: &ConvId, link: &ConvLink, keys: &ConvKeyBundle, now: i64) -> OpResult {
        self.require_active(s)?;
        let body = self.check_author(s, link)?;
        let ConvChange::JoinByInvite { invite, .. } = &body.change else {
            return Err(fail(ErrorCode::Invalid, "这一环应是凭邀请加入"));
        };
        if &body.conv != conv {
            return Err(fail(ErrorCode::Invalid, "会话对不上"));
        }
        let row: Option<(String, i64, i64)> = self
            .conn
            .query_row("SELECT conv, generation, expires_ms FROM invites WHERE invite = ?1", params![invite.as_str()], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .optional()
            .db()?;
        let (invite_conv, generation, expires_ms) = row.ok_or_else(|| fail(ErrorCode::NotFound, "群邀请无效或已失效"))?;
        if invite_conv != conv.as_str() {
            return Err(fail(ErrorCode::Invalid, "群邀请不属于这个会话"));
        }
        if expires_ms <= now {
            return Err(fail(ErrorCode::Expired, "群邀请已过期"));
        }
        let mut state = self.conv_state(conv)?.ok_or_else(|| fail(ErrorCode::NotFound, "没有这个会话"))?;
        let mut uids = link_uids(std::slice::from_ref(link));
        uids.extend(state.members.keys().cloned());
        self.preload_accounts(&uids)?;
        let log = self.last_log + 1;
        state.apply(link, log, &CacheDirectory { accounts: &self.accounts }).map_err(|e| fail(ErrorCode::Invalid, e.to_string()))?;
        let expected: BTreeSet<Uid> = [s.uid.clone()].into();
        self.check_conv_bundle(keys, generation as u32, &expected, Some(false))?;
        // 推给库里现在的成员和加入的人（与接签名链同一个口径）。
        let mut recipients: BTreeSet<Uid> = self.current_members(conv)?.into_iter().collect();
        recipients.insert(s.uid.clone());
        let tx = self.conn.unchecked_transaction().db()?;
        let event = Self::write_conv_link(&tx, conv, state.seq, link, now, &recipients)?;
        if event.log != log {
            return Err(internal("日志序号与预期不符"));
        }
        tx.execute("INSERT INTO conv_members(conv, uid, joined_log) VALUES(?1, ?2, ?3)", params![conv.as_str(), s.uid.as_str(), log as i64])
            .db()?;
        Self::write_conv_bundle(&tx, conv, keys)?;
        let (keys_event, key_uids) = Self::write_keys_event(&tx, conv, keys, now)?;
        tx.commit().db()?;
        self.last_log = keys_event.log;
        self.convs.insert(conv.clone(), state);
        tracing::info!(conv = %conv, "凭群邀请加入");
        let mut effects = Effects::default();
        for uid in &recipients {
            effects.push(Target::Account(uid.clone()), Push::Event { event: event.clone() });
        }
        for uid in key_uids {
            effects.push(Target::Account(uid), Push::Event { event: keys_event.clone() });
        }
        Ok((Response::Appended { log: keys_event.log }, effects))
    }
}
