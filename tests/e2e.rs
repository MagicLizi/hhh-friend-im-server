//! 端到端：真的服务器 + 真的客户端库（插件将来走的同一条路），按契约 S1–S13、S16、S19–S22 的服务器链路走一遍；
//! 再从服务器这一侧验：坏请求被拒且整批不生效、服务器私自改数据会被插件识破、消息和文件从不落到服务器的数据目录（friend-im v10）。
//!
//! 每个测试自己起一套假 Cindy + 服务器，互不影响。设 `RUST_LOG=friend_im_server=debug,friend_im_client=debug` 可看日志。

mod common;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use common::*;
use friend_im_client::core::account::{self, AccountChange, AccountLink, AccountLinkBody, AccountState, DeviceEntry, ProofRole, device_proof};
use friend_im_client::core::bytes::{self, Bytes, Fixed};
use friend_im_client::core::conv::{self, ConvChange, ConvKind, ConvLink, ConvLinkBody, GroupProfile, MemberRef, RemoveReason, Via};
use friend_im_client::core::crypto;
use friend_im_client::core::ids::{ConvId, MsgId, Uid};
use friend_im_client::core::peer::SealedFrame;
use friend_im_client::core::keys::{AccountSeed, ConvSeed, DeviceSecret, RecoveryKey, account_seed_info, conv_seed_info};
use friend_im_client::core::limits;
use friend_im_client::core::message::{Content, Part};
use friend_im_client::core::wire::{AccountKeyBundle, DeviceKeyBox, DeviceStatus, ErrorCode, JoinCloseReason, Request, Response};
use friend_im_client::core::words::JoinCode;
use friend_im_client::{AccountNotice, ClientError, ConvNotice, Credentials, FriendLink, InviteLink, Notice, Update};
use rusqlite::params;
use serde_json::json;

fn now_ms() -> i64 {
    friend_im_server::now_ms()
}

/// 某台电脑的成员身份（账号 + 当前身份指纹）。
async fn me(peer: &Peer) -> MemberRef {
    MemberRef {
        uid: peer.client.uid().clone(),
        eldest: peer.client.friend_link(false).await.unwrap().eldest,
    }
}

async fn send(peer: &Peer, conv: &ConvId, body: &str) -> MsgId {
    let id = MsgId::random();
    peer.send_as(conv, &id, text(body)).await.unwrap_or_else(|e| panic!("{} 发「{body}」失败：{e}", peer.name));
    id
}

async fn wait_notice(peer: &mut Peer, conv: &ConvId, what: &str, f: impl Fn(&ConvNotice) -> bool) -> Notice {
    peer.wait(what, |u| match u {
        Update::Conv(n) if &n.conv == conv && f(&n.change) => Some(n.clone()),
        _ => None,
    })
    .await
}

/// 等这台电脑的宿主里这个会话的文字正好是这些（对账补齐是异步的）。
async fn wait_texts(peer: &Peer, conv: &ConvId, expected: &[&str]) {
    let deadline = Instant::now() + WAIT;
    loop {
        let got = peer.texts(conv).await;
        if got == expected {
            return;
        }
        assert!(Instant::now() < deadline, "{} 等不到 {expected:?}，现在是 {got:?}", peer.name);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

fn wrong_code(right: &JoinCode) -> JoinCode {
    loop {
        let code = JoinCode::generate();
        if &code != right {
            return code;
        }
    }
}

/// 一直重试到 `f` 成功或超时（等别的电脑在后台做完一件事，比如补发钥匙）。
async fn eventually<T>(what: &str, mut f: impl AsyncFnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(value) = f().await {
            return value;
        }
        assert!(Instant::now() < deadline, "等不到{what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---------- S1、S2、S4、S5、S21：开通、加电脑、电脑增减提醒、我的电脑 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_computer_provisions_and_second_joins_with_four_words() {
    let env = Env::start().await;

    // S1：第一台电脑用 Cindy 凭证登录，账号第一次用：开通，拿到恢复密钥。
    let mut pc1 = env.device("alice", "台式机").await;
    pc1.wait("连上（未开通）", |u| matches!(u, Update::Connected { status: DeviceStatus::Unprovisioned }).then_some(())).await;
    pc1.wait("拿到登录凭证", |u| matches!(u, Update::CredentialsChanged).then_some(())).await;
    let started = Instant::now();
    let recovery = pc1.client.provision().await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(5), "开通要在 5 秒内：{:?}", started.elapsed());
    assert_eq!(recovery.to_words().split(' ').count(), 12);
    pc1.wait("开通提醒", |u| matches!(u, Update::Account { notice: AccountNotice::Started { reset: false }, .. }).then_some(())).await;
    assert_eq!(pc1.client.name().await, "alice");
    assert!(pc1.client.provision().await.is_err(), "开通只能一次");

    // 先和 bob 聊几条，后面看新电脑能不能补齐历史。
    let (mut bob, _) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &pc1.client).await;
    for i in 1..=3 {
        send(&pc1, &dm, &format!("加电脑前的第 {i} 条")).await;
    }
    bob.wait_text("加电脑前的第 3 条").await;

    // S2：第二台电脑登录同一个账号：等批准，拿不到任何会话内容。
    let mut pc2 = env.device("alice", "笔记本").await;
    pc2.wait("连上（待批准）", |u| matches!(u, Update::Connected { status: DeviceStatus::Pending }).then_some(())).await;
    assert!(pc2.client.convs().await.is_err());
    let started = pc2.client.join_start().await.unwrap();
    let words = started.code;
    assert_eq!(words.as_str().split(' ').count(), 4);
    // 契约 v3：回执带上已加入的电脑与此刻在线，这台新电脑自己不算。
    let listed: Vec<_> = started.devices.expect("带已登录的电脑").into_iter().map(|d| (d.name, d.online)).collect();
    assert_eq!(listed, [("台式机".to_owned(), true)]);
    let request = pc1
        .wait("新电脑请求加入", |u| match u {
            Update::JoinRequested(info) => Some(info.clone()),
            _ => None,
        })
        .await;
    assert_eq!(request.name, "笔记本");
    assert_eq!(request.device, pc2.client.credentials().await.device.public());

    // S21：旧电脑输错了：两边都知道，可以再输。
    pc1.client.join_respond(&request.request, wrong_code(&words).as_str()).await.unwrap();
    pc2.wait("和旧电脑上输的不一致", |u| matches!(u, Update::JoinWrongCode { attempt: 2, .. }).then_some(())).await;
    pc1.wait("可以再输", |u| matches!(u, Update::JoinRetry { attempt: 2, .. }).then_some(())).await;

    // 输对（大小写不论）：新电脑随即可用。
    let approving = Instant::now();
    pc1.client.join_respond(&request.request, &words.as_str().to_uppercase()).await.unwrap();
    pc2.wait("批准", |u| matches!(u, Update::JoinClosed { reason: JoinCloseReason::Approved, .. }).then_some(())).await;
    pc2.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    pc1.wait("批准", |u| matches!(u, Update::JoinClosed { reason: JoinCloseReason::Approved, .. }).then_some(())).await;
    assert!(pc1.client.join_requests().await.unwrap().is_empty(), "请求随之消失");

    // 30 秒内出现会话列表；以前的消息全部补得到。
    let convs = pc2.client.convs().await.unwrap();
    assert!(convs.iter().any(|c| c.conv == dm && c.member));
    assert!(approving.elapsed() < Duration::from_secs(30));
    wait_texts(&pc2, &dm, &["加电脑前的第 1 条", "加电脑前的第 2 条", "加电脑前的第 3 条"]).await;
    let notices = pc2.client.notices(&dm).await.unwrap();
    assert!(matches!(
        notices.first(),
        Some(Notice { change: ConvNotice::Created { kind: ConvKind::Dm, via: Some(Via::FriendLink), .. }, .. })
    ));

    // S4：本账号所有电脑都收到“新增了电脑”。
    for pc in [&mut pc1, &mut pc2] {
        pc.wait("新增电脑的提醒", |u| matches!(u, Update::Account { notice: AccountNotice::DeviceAdded { name, .. }, .. } if name == "笔记本").then_some(()))
            .await;
    }

    // S14：同一账号的其它电脑同步看到。
    send(&bob, &dm, "批准后的消息").await;
    pc1.wait_text("批准后的消息").await;
    pc2.wait_text("批准后的消息").await;
    send(&pc2, &dm, "笔记本发的").await;
    pc1.wait_text("笔记本发的").await;
    bob.wait_text("笔记本发的").await;

    // S5：我的电脑：列出全部电脑；移除笔记本，它立刻退出。
    let devices = pc1.client.devices().await.unwrap();
    assert_eq!(devices.iter().map(|d| d.name.as_str()).collect::<Vec<_>>(), ["台式机", "笔记本"]);
    assert!(devices.iter().all(|d| d.online && d.joined_ms > 0 && d.last_seen_ms > 0));
    let pc2_id = pc2.client.device_id().clone();
    let pc2_creds = pc2.client.credentials().await;
    pc1.client.remove_device(&pc2_id).await.unwrap();
    pc2.wait("被移出账号", |u| matches!(u, Update::DeviceRemoved).then_some(())).await;
    pc1.wait("已移除电脑的提醒", |u| matches!(u, Update::Account { notice: AccountNotice::DeviceRemoved { name, .. }, .. } if name == "笔记本").then_some(()))
        .await;
    assert_eq!(pc1.client.devices().await.unwrap().len(), 1);
    assert!(!pc2.client.is_connected());
    // 被移除的电脑再也登录不上，凭登录凭证或 Cindy 凭证都不行，两种都照实回“已被移出”。
    let err = Peer::connect(&env.server.url(), "笔记本", pc2_creds.clone(), None, None).await.err().unwrap();
    assert_eq!(code(&err), Some(ErrorCode::DeviceRemoved));
    let err = Peer::connect(&env.server.url(), "笔记本", pc2_creds, Some(env.cindy.token("alice", "alice")), None)
        .await
        .err()
        .unwrap();
    assert_eq!(code(&err), Some(ErrorCode::DeviceRemoved));

    // 之后的新消息用新一代钥匙：会话钥匙封装给账号钥匙第 2 代，被移除的笔记本只有第 1 代。
    send(&pc1, &dm, "移除笔记本之后").await;
    bob.wait_text("移除笔记本之后").await;
    let (generation, account_generation): (i64, i64) = env
        .server
        .db()
        .query_row(
            "SELECT generation, account_generation FROM conv_keys WHERE conv = ?1 AND uid = ?2 ORDER BY generation DESC LIMIT 1",
            params![dm.as_str(), pc1.client.uid().as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((generation, account_generation), (2, 2));
}

// ---------- S3、S6、S22：恢复密钥加入、重新生成恢复密钥、退出登录 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_key_joins_replacement_kills_old_key_and_logout_removes_computer() {
    let env = Env::start().await;
    let (mut pc1, recovery) = env.account("alice", "台式机").await;
    let (bob, _) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &pc1.client).await;
    send(&bob, &dm, "恢复前的消息").await;

    // S3：新电脑用恢复密钥加入（12 个词）：账号不用旧电脑批准；以前的消息要等另一台设备或聊天对象在线才补得到。
    let mut pc2 = env.device("alice", "笔记本").await;
    pc2.client.join_with_recovery(&recovery.to_words()).await.unwrap();
    pc2.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    pc1.wait("新增电脑的提醒", |u| matches!(u, Update::Account { notice: AccountNotice::DeviceAdded { name, .. }, .. } if name == "笔记本").then_some(()))
        .await;
    wait_texts(&pc2, &dm, &["恢复前的消息"]).await;

    // 抄错一个词：校验发现，不会登错。
    let mut pc3 = env.device("alice", "平板").await;
    let mut words: Vec<String> = recovery.to_words().split(' ').map(String::from).collect();
    words.swap(0, 1);
    assert!(pc3.client.join_with_recovery(&words.join(" ")).await.is_err());
    // 别人的恢复密钥（格式对）：拒绝。这一步会让新电脑先取一次签名链，下面换恢复密钥后它手里的就是旧的了。
    let err = pc3.client.join_with_recovery(&RecoveryKey::generate().to_words()).await.unwrap_err();
    assert!(matches!(err, ClientError::State(_)), "{err}");

    // S6：重新生成恢复密钥：旧的立即失效。
    let replaced = pc1.client.replace_recovery().await.unwrap();
    pc2.wait("换了恢复密钥的提醒", |u| matches!(u, Update::Account { notice: AccountNotice::RecoveryReplaced, .. }).then_some(())).await;
    let err = pc3.client.join_with_recovery(&recovery.to_groups()).await.unwrap_err();
    assert!(matches!(err, ClientError::State(_)), "旧恢复密钥应被拒：{err}");
    pc3.client.join_with_recovery(&replaced.to_groups()).await.unwrap();
    pc3.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    wait_texts(&pc3, &dm, &["恢复前的消息"]).await;
    // 换代后的消息三台都读得到（换恢复密钥时账号钥匙换了一代）。
    send(&bob, &dm, "换恢复密钥之后").await;
    for pc in [&mut pc1, &mut pc2, &mut pc3] {
        pc.wait_text("换恢复密钥之后").await;
    }

    // S22：退出登录：这台从“我的电脑”里移除，之后登录要重新批准。
    let pc3_creds = pc3.client.credentials().await;
    pc3.client.logout().await.unwrap();
    assert!(!pc3.client.is_connected());
    for pc in [&mut pc1, &mut pc2] {
        pc.wait("已移除电脑的提醒", |u| matches!(u, Update::Account { notice: AccountNotice::DeviceRemoved { name, .. }, .. } if name == "平板").then_some(()))
            .await;
        // 在线的电脑当场补换一代账号钥匙：退出的那台读不到之后的消息。
        pc.wait("补换账号钥匙", |u| matches!(u, Update::Account { notice: AccountNotice::KeyRotated, .. }).then_some(())).await;
    }
    assert_eq!(pc1.client.devices().await.unwrap().len(), 2);
    let err = Peer::connect(&env.server.url(), "平板", pc3_creds.clone(), None, None).await.err().unwrap();
    assert_eq!(code(&err), Some(ErrorCode::DeviceRemoved));
    // 用同一套电脑钥匙再登录 Cindy 也不行：要换一套新钥匙重新批准。
    let err = Peer::connect(&env.server.url(), "平板", pc3_creds, Some(env.cindy.token("alice", "alice")), None)
        .await
        .err()
        .unwrap();
    assert_eq!(code(&err), Some(ErrorCode::DeviceRemoved));
    let account_generation: i64 = env
        .server
        .db()
        .query_row("SELECT MAX(generation) FROM account_keys WHERE uid = ?1", params![pc1.client.uid().as_str()], |r| r.get(0))
        .unwrap();
    assert_eq!(account_generation, 3, "换恢复密钥一次、退出后补换一次");
}

// ---------- S7、S16：全丢了只能重置；好友看到重置提示 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_starts_over_leaves_groups_and_friends_see_it() {
    let env = Env::start().await;
    let (mut old_pc, _) = env.account("alice", "旧电脑").await;
    let (mut bob, _) = env.account("bob", "bob 的电脑").await;
    let (mut carol, _) = env.account("carol", "carol 的电脑").await;
    let alice_uid = old_pc.client.uid().clone();
    let dm = approved_dm(&bob.client, &old_pc.client).await;
    approved_dm(&carol.client, &old_pc.client).await;
    let group = old_pc
        .client
        .create_group(&GroupProfile { name: "周末爬山".into(), ..Default::default() }, &[bob.client.uid().clone(), carol.client.uid().clone()])
        .await
        .unwrap();
    send(&old_pc, &dm, "重置前的私聊").await;
    send(&old_pc, &group, "重置前的群消息").await;
    bob.wait_text("重置前的私聊").await;
    bob.wait_text("重置前的群消息").await;
    carol.wait_text("重置前的群消息").await;
    let old_eldest = me(&old_pc).await.eldest;

    // S7：新电脑上既没有旧电脑可批准、也没有恢复密钥：重置。
    let mut fresh = env.device("alice", "新电脑").await;
    let recovery = fresh.client.reset().await.unwrap();
    assert_eq!(recovery.to_words().split(' ').count(), 12);
    fresh.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    old_pc.wait("旧电脑被移出", |u| matches!(u, Update::DeviceRemoved).then_some(())).await;
    assert_ne!(me(&fresh).await.eldest, old_eldest, "身份指纹变了");

    // S16：好友看到“重置了账号”。
    for peer in [&mut bob, &mut carol] {
        peer.wait("alice 重置了账号", |u| matches!(u, Update::Account { uid, notice: AccountNotice::Started { reset: true }, .. } if uid == &alice_uid).then_some(()))
            .await;
    }

    // 私聊：bob 下一条消息前改记 alice 的新身份并换代，私聊里出现提示。
    send(&bob, &dm, "重置后的私聊").await;
    wait_notice(&mut bob, &dm, "私聊里的重置提示", |c| matches!(c, ConvNotice::MemberReset { uid } if uid == &alice_uid)).await;
    wait_notice(&mut fresh, &dm, "私聊里的重置提示", |c| matches!(c, ConvNotice::MemberReset { uid } if uid == &alice_uid)).await;
    fresh.wait_text("重置后的私聊").await;
    // 以前的消息读不出来：对账时验不过，不进宿主。
    bob.settle(Duration::from_millis(300)).await;
    assert_eq!(fresh.texts(&dm).await, ["重置后的私聊"]);

    // 群：已经退出；群里下一次发消息前把她移出并换代，她读不到之后的消息、也发不了。
    let convs = fresh.client.convs().await.unwrap();
    assert!(convs.iter().any(|c| c.conv == group && !c.member));
    send(&bob, &group, "alice 走了之后").await;
    wait_notice(&mut carol, &group, "群里的移出提示", |c| matches!(c, ConvNotice::Removed { uid, reason: RemoveReason::Reset } if uid == &alice_uid)).await;
    carol.wait_text("alice 走了之后").await;
    // 按库里的记录她重置时就离开了群：群里之后的变化（补签的移出、换代）不再推给她。
    let after_leave: i64 = env
        .server
        .db()
        .query_row(
            "SELECT COUNT(*) FROM feed f JOIN conv_links l ON l.log = f.seq WHERE f.uid = ?1 AND l.conv = ?2 \
             AND l.log > (SELECT MIN(left_log) FROM conv_members WHERE conv = ?2 AND uid = ?1)",
            params![alice_uid.as_str(), group.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(after_leave, 0);
    fresh.settle(Duration::from_millis(300)).await;
    assert!(!fresh.log.iter().any(|u| matches!(u, Update::ConvRejected { .. })), "{:#?}", fresh.log);
    assert!(fresh.texts(&group).await.is_empty());
    assert!(fresh.send_as(&group, &MsgId::random(), text("我还在吗")).await.is_err());

    // 需要别人重新邀请：bob 邀请回来，又能在群里聊天。
    bob.client.invite(&group, std::slice::from_ref(&alice_uid)).await.unwrap();
    wait_notice(&mut fresh, &group, "被邀请回群", |c| matches!(c, ConvNotice::Added { members } if members.contains(&alice_uid))).await;
    send(&fresh, &group, "我回来了").await;
    bob.wait_text("我回来了").await;
    carol.wait_text("我回来了").await;
}

// ---------- S8–S14：好友链接、私聊、群、邀请、移出与退群、各种消息 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn friends_groups_and_every_message_kind() {
    let env = Env::start().await;
    let (mut alice, _) = env.account("alice", "alice 的电脑").await;
    let (mut bob, _) = env.account("bob", "bob 的电脑").await;
    let (mut carol, _) = env.account("carol", "carol 的电脑").await;
    let (mut dave, _) = env.account("dave", "dave 的电脑").await;
    let (mut erin, _) = env.account("erin", "erin 的电脑").await;
    let (frank, _) = env.account("frank", "frank 的电脑").await;
    let uid = |p: &Peer| p.client.uid().clone();

    // S8：好友链接：同一条反复取不变；换一条后旧的立即失效。
    let first = alice.client.friend_link(false).await.unwrap();
    assert_eq!(alice.client.friend_link(false).await.unwrap(), first);
    assert!(first.to_string().starts_with(&format!("{}/f/", env.server.url())));
    let link = alice.client.friend_link(true).await.unwrap();
    assert_ne!(link.token, first.token);
    assert_eq!(code(&bob.client.add_friend(&first).await.unwrap_err()), Some(ErrorCode::NotFound));
    // 链接里的身份指纹被改过：拒绝，不会加错人。
    let mut forged = link.clone();
    forged.eldest = Fixed([7; 32]);
    assert!(matches!(bob.client.add_friend(&forged).await.unwrap_err(), ClientError::Verify(_)));

    // S9 v9：接收方批准才出现私聊，创建环由批准者签名，来源仍是好友链接。
    let dm = approved_link(&bob.client, &alice.client, &FriendLink::parse(&link.to_string()).unwrap()).await;
    let accepting_uid = uid(&alice);
    for peer in [&mut alice, &mut bob] {
        let notice = wait_notice(peer, &dm, "新私聊", |c| matches!(c, ConvNotice::Created { kind: ConvKind::Dm, via: Some(Via::FriendLink), .. })).await;
        assert_eq!(notice.author, accepting_uid);
    }
    // 重复加、反过来加，都是同一个私聊。
    assert_eq!(bob.client.add_friend(&link).await.unwrap(), dm);
    assert_eq!(approved_dm(&alice.client, &bob.client).await, dm);
    let own = alice.client.friend_link(false).await.unwrap();
    assert!(matches!(alice.client.add_friend(&own).await.unwrap_err(), ClientError::State(_)), "不能加自己");

    // S14：文字、@人、回复、图片、文件、撤回。
    let hello = send(&alice, &dm, "你好").await;
    bob.wait_text("你好").await;
    let mention = Content::Post {
        parts: vec![Part::Mention { uid: Some(uid(&bob)), name: Some("bob".into()), everyone: false }, Part::Text { text: " 在吗".into() }],
        reply_to: Some(hello.clone()),
    };
    alice.send_as(&dm, &MsgId::random(), mention.clone()).await.unwrap();
    let got = bob.wait_text(" 在吗").await;
    assert_eq!(got.content, mention);
    assert_eq!(got.sender, uid(&alice));
    // 同一次发送重试：所有设备上只有一条。
    let retry = MsgId::random();
    alice.send_as(&dm, &retry, text("只出现一次")).await.unwrap();
    alice.send_as(&dm, &retry, text("只出现一次")).await.unwrap();
    // 撤回：两边都变成墓碑，撤回的不会回来。
    let oops = send(&alice, &dm, "发错了").await;
    bob.wait_text("发错了").await;
    alice.recall(&dm, &oops).await.unwrap();
    eventually("bob 看到撤回", async || bob.is_recalled(&dm, &oops).then_some(())).await;
    assert!(alice.is_recalled(&dm, &oops));
    assert!(!bob.texts(&dm).await.contains(&"发错了".to_owned()));
    // 不能撤回别人的消息。
    assert!(matches!(bob.recall(&dm, &hello).await.unwrap_err(), ClientError::State(_)));
    bob.settle(Duration::from_millis(300)).await;
    assert!(!alice.is_recalled(&dm, &hello), "伪造的撤回不生效");
    assert_eq!(bob.texts(&dm).await.iter().filter(|t| *t == "只出现一次").count(), 1);

    // S11：新建群：成员是认识的人；群名只有成员看得到。
    approved_dm(&carol.client, &alice.client).await;
    let photo = friend_im_client::core::file::FileRef { id: friend_im_client::core::ids::FileId::random(), key: Fixed([1; 32]), size: 4, name: "群头像.png".into(), mime: "image/png".into(), sha256: None };
    let profile = GroupProfile { name: "周末爬山".into(), description: "每周六早上".into(), avatar: Some(photo) };
    let group = alice.client.create_group(&profile, &[uid(&bob), uid(&carol)]).await.unwrap();
    let alice_uid = uid(&alice);
    let expected: BTreeSet<Uid> = [uid(&alice), uid(&bob), uid(&carol)].into();
    for peer in [&mut alice, &mut bob, &mut carol] {
        let notice = wait_notice(peer, &group, "建群提示", |c| matches!(c, ConvNotice::Created { kind: ConvKind::Group, .. })).await;
        let ConvNotice::Created { members, .. } = notice.change else { unreachable!() };
        assert_eq!(members.into_iter().collect::<BTreeSet<_>>(), expected);
        assert_eq!(notice.author, alice_uid);
    }
    assert_eq!(bob.client.group_profile(&group).await.unwrap(), Some(profile.clone()));
    // 不认识的人拉不进群。
    assert!(alice.client.create_group(&profile, &[uid(&frank)]).await.is_err());
    let everyone = Content::Post {
        parts: vec![Part::Mention { uid: None, name: None, everyone: true }, Part::Text { text: " 群里第一条".into() }],
        reply_to: None,
    };
    alice.send_as(&group, &MsgId::random(), everyone).await.unwrap();
    bob.wait_text(" 群里第一条").await;
    carol.wait_text(" 群里第一条").await;

    // S10：和群成员私聊，不用先加好友；身份以群里的为准。
    let request=friend_im_client::core::ids::RequestId::random();
    bob.client.request_group_friend(&request,&uid(&carol),&group,"").await.unwrap();
    let dm_bc=carol.client.accept_friend(&request).await.unwrap().record.conversation.unwrap();
    wait_notice(&mut carol, &dm_bc, "群里认识的私聊", |c| matches!(c, ConvNotice::Created { via: Some(Via::Group { conv }), .. } if conv == &group)).await;
    send(&bob, &dm_bc, "群里看到你了").await;
    carol.wait_text("群里看到你了").await;

    // S12：任何成员都能邀请人；新成员看得到以前的全部消息。
    approved_dm(&dave.client, &carol.client).await;
    carol.client.invite(&group, &[uid(&dave)]).await.unwrap();
    for peer in [&mut alice, &mut bob, &mut dave] {
        let notice = wait_notice(peer, &group, "邀请提示", |c| matches!(c, ConvNotice::Added { .. })).await;
        assert_eq!(notice.author, uid(&carol));
    }
    wait_texts(&dave, &group, &[" 群里第一条"]).await;
    assert_eq!(dave.client.group_profile(&group).await.unwrap().map(|p| p.name), Some("周末爬山".into()));
    // 群邀请链接：凭链接加入，不用等成员在线。
    let invite = alice.client.create_invite_link(&group).await.unwrap();
    let invite_text = invite.to_string();
    assert!(invite_text.contains("/g/"));
    let joined = erin.client.join_by_invite(&InviteLink::parse(&invite_text).unwrap()).await.unwrap();
    assert_eq!(joined, group);
    wait_notice(&mut bob, &group, "凭链接加入", |c| matches!(c, ConvNotice::JoinedByInvite)).await;
    wait_texts(&erin, &group, &[" 群里第一条"]).await;

    // S13：只有群主能移出；移出后对方保留之前的、读不到之后的，也发不了。
    assert!(bob.client.kick(&group, &uid(&erin)).await.is_err());
    alice.client.kick(&group, &uid(&dave)).await.unwrap();
    for peer in [&mut bob, &mut dave] {
        wait_notice(peer, &group, "移出提示", |c| matches!(c, ConvNotice::Removed { reason: RemoveReason::Kicked, .. })).await;
    }
    send(&alice, &group, "移出 dave 之后").await;
    bob.wait_text("移出 dave 之后").await;
    erin.wait_text("移出 dave 之后").await;
    assert_eq!(dave.texts(&group).await, [" 群里第一条"]);
    assert!(dave.client.convs().await.unwrap().iter().any(|c| c.conv == group && !c.member));
    assert!(dave.send_as(&group, &MsgId::random(), text("还能发吗")).await.is_err());
    dave.settle(Duration::from_millis(300)).await;
    assert!(!dave.texts(&group).await.contains(&"移出 dave 之后".to_owned()), "被移出的人从任何路径都拿不到之后的");
    // 群里换过代之后凭旧链接加入的人：拿到的是发邀请时那一代，别的成员自动补发当前一代，照样读得到新消息。
    frank.client.join_by_invite(&InviteLink::parse(&invite_text).unwrap()).await.unwrap();
    eventually("frank 拿到当前一代钥匙和以前的消息", async || frank.texts(&group).await.contains(&"移出 dave 之后".to_string()).then_some(())).await;
    send(&frank, &group, "frank 来了").await;
    alice.wait_text("frank 来了").await;
    // 作废链接后不能再凭它加入。
    alice.client.revoke_invite(&group, &invite.invite).await.unwrap();
    assert!(dave.client.join_by_invite(&InviteLink::parse(&invite_text).unwrap()).await.is_err());

    // 退群：剩下的人下一次发消息前自动换代，退出的人读不到之后的。
    carol.client.leave(&group).await.unwrap();
    wait_notice(&mut alice, &group, "退群提示", |c| matches!(c, ConvNotice::Left)).await;
    send(&alice, &group, "carol 走了之后").await;
    bob.wait_text("carol 走了之后").await;
    assert!(!carol.texts(&group).await.contains(&"carol 走了之后".to_string()));
    // 退群的人凭新的群邀请链接回来：她手里那份签名链停在退群时，要先用邀请回执补齐之后的环再签加入。
    let again = alice.client.create_invite_link(&group).await.unwrap();
    carol.client.join_by_invite(&InviteLink::parse(&again.to_string()).unwrap()).await.unwrap();
    wait_notice(&mut alice, &group, "退群的人凭链接回来", |c| matches!(c, ConvNotice::JoinedByInvite)).await;
    eventually("carol 回来后读得到当前一代的消息", async || carol.texts(&group).await.contains(&"carol 走了之后".to_string()).then_some(())).await;
    send(&alice, &group, "carol 回来之后").await;
    carol.wait_text("carol 回来之后").await;
    // 改群名：成员看得到新的。
    let renamed = GroupProfile { name: "周日爬山".into(), ..Default::default() };
    bob.client.set_group_profile(&group, &renamed).await.unwrap();
    wait_notice(&mut alice, &group, "改群资料提示", |c| matches!(c, ConvNotice::ProfileChanged)).await;
    assert_eq!(alice.client.group_profile(&group).await.unwrap(), Some(renamed));

    // 所有在群里的人，消息逐条一样、顺序一样。
    eventually("三个人一模一样", async || {
        let reference = alice.texts(&group).await;
        (bob.texts(&group).await == reference && erin.texts(&group).await == reference).then_some(())
    })
    .await;
}

// ---------- S19：断网、服务器停了：恢复后补收，不丢不重 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnects_and_catches_up_without_loss_or_duplicates() {
    let env = Env::start().await;
    // alice 直连服务器，bob 经过可以断网的代理。
    let direct = format!("http://{}", env.server.direct);
    let mut alice = Peer::connect(&direct, "alice 的电脑", Credentials::generate(), Some(env.cindy.token("alice", "alice")), None).await.unwrap();
    alice.client.provision().await.unwrap();
    let (mut bob, _) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &alice.client).await;
    send(&alice, &dm, "断网前").await;
    bob.wait_text("断网前").await;

    env.server.proxy.cut();
    bob.wait("断开", |u| matches!(u, Update::Disconnected).then_some(())).await;
    // 断开时也能发：本机保存了就算发出，等对账送达。
    let pending = MsgId::random();
    bob.send_as(&dm, &pending, text("断网时发的")).await.unwrap();
    for i in 0..30 {
        send(&alice, &dm, &format!("断网期间 {i}")).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    env.server.proxy.restore();
    bob.wait("重新连上", |u| matches!(u, Update::Connected { status: DeviceStatus::Active }).then_some(())).await;
    alice.wait_text("断网时发的").await;
    for i in 0..30 {
        bob.wait_text(&format!("断网期间 {i}")).await;
    }
    eventually("两边一模一样", async || (alice.texts(&dm).await == bob.texts(&dm).await).then_some(())).await;
    let during: Vec<String> = bob.texts(&dm).await.into_iter().filter(|t| t.starts_with("断网期间")).collect();
    assert_eq!(during, (0..30).map(|i| format!("断网期间 {i}")).collect::<Vec<_>>(), "按顺序、每条一次");
    // 用户在失败记录里重试：同一个编号只有一条。
    bob.send_as(&dm, &pending, text("断网时发的")).await.unwrap();
    bob.settle(Duration::from_millis(300)).await;
    assert_eq!(alice.texts(&dm).await.iter().filter(|t| *t == "断网时发的").count(), 1);

    // 关掉插件，带着聊天记录和同步位置重新打开：补收关着的这段时间的。
    let position = bob.position().expect("有同步位置");
    let creds = bob.client.credentials().await;
    let host = bob.host.clone();
    bob.client.close();
    for i in 0..5 {
        send(&alice, &dm, &format!("关着的时候 {i}")).await;
    }
    let mut reopened = Peer::connect_with(host, &env.server.url(), "bob 的电脑", creds, None, Some(position)).await.unwrap();
    for i in 0..5 {
        reopened.wait_text(&format!("关着的时候 {i}")).await;
    }
    eventually("重开后一模一样", async || (alice.texts(&dm).await == reopened.texts(&dm).await).then_some(())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_restart_changes_nothing_because_it_keeps_no_messages() {
    let mut env = Env::start().await;
    let (mut alice, _) = env.account("alice", "alice 的电脑").await;
    let (mut bob, _) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &alice.client).await;
    send(&alice, &dm, "重启前").await;
    bob.wait_text("重启前").await;

    env.server.restart().await;
    alice.wait("断开", |u| matches!(u, Update::Disconnected).then_some(())).await;
    alice.wait("重新连上", |u| matches!(u, Update::Connected { status: DeviceStatus::Active }).then_some(())).await;
    bob.wait("重新连上", |u| matches!(u, Update::Connected { status: DeviceStatus::Active }).then_some(())).await;
    send(&alice, &dm, "重启后").await;
    bob.wait_text("重启后").await;
    assert_eq!(bob.texts(&dm).await, ["重启前", "重启后"]);
}

// ---------- S20、S21：拒绝、取消、连续输错 5 次 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_requests_end_on_reject_cancel_or_five_wrong_codes() {
    let env = Env::start().await;
    let (mut pc1, _) = env.account("alice", "台式机").await;
    let mut pc2 = env.device("alice", "笔记本").await;
    let requested = |u: &Update| match u {
        Update::JoinRequested(info) => Some(info.clone()),
        _ => None,
    };

    // S20：旧电脑拒绝：两边同时结束，旧电脑上的请求随之消失；新电脑可以重新请求。
    pc2.client.join_start().await.unwrap();
    let request = pc1.wait("请求加入", requested).await;
    assert_eq!(pc1.client.join_requests().await.unwrap().len(), 1);
    pc1.client.join_reject(&request.request).await.unwrap();
    pc2.wait("被拒绝", |u| matches!(u, Update::JoinClosed { reason: JoinCloseReason::Rejected, .. }).then_some(())).await;
    pc1.wait("请求结束", |u| matches!(u, Update::JoinClosed { reason: JoinCloseReason::Rejected, .. }).then_some(())).await;
    assert!(pc1.client.join_requests().await.unwrap().is_empty());

    // 同时只能有一个请求：别的新电脑要等这个结束。
    let code = pc2.client.join_start().await.unwrap().code;
    let request = pc1.wait("请求加入", requested).await;
    let pc3 = env.device("alice", "平板").await;
    assert_eq!(code_of(pc3.client.join_start().await), Some(ErrorCode::Conflict));

    // S21：连续输错 5 次：这次请求作废，两边都提示重新开始；作废后输对也没用。
    for attempt in 1..=5u32 {
        pc1.client.join_respond(&request.request, wrong_code(&code).as_str()).await.unwrap();
        if attempt < 5 {
            pc2.wait("不一致", |u| matches!(u, Update::JoinWrongCode { attempt: a, .. } if *a == attempt + 1).then_some(())).await;
            pc1.wait("可以再输", |u| matches!(u, Update::JoinRetry { attempt: a, .. } if *a == attempt + 1).then_some(())).await;
        }
    }
    pc2.wait("输错太多次", |u| matches!(u, Update::JoinClosed { reason: JoinCloseReason::TooManyAttempts, .. }).then_some(())).await;
    pc1.wait("输错太多次", |u| matches!(u, Update::JoinClosed { reason: JoinCloseReason::TooManyAttempts, .. }).then_some(())).await;
    assert!(pc1.client.join_respond(&request.request, code.as_str()).await.is_err());
    assert_eq!(pc2.client.status().await, DeviceStatus::Pending);

    // 新电脑不等了（改用恢复密钥）：旧电脑上的请求随之消失。
    pc2.client.join_start().await.unwrap();
    pc1.wait("请求加入", requested).await;
    pc2.client.join_cancel().await.unwrap();
    pc1.wait("请求结束", |u| matches!(u, Update::JoinClosed { reason: JoinCloseReason::Cancelled, .. }).then_some(())).await;
    // 结束后另一台新电脑可以发起了。
    pc3.client.join_start().await.unwrap();
}

fn code_of<T>(result: Result<T, ClientError>) -> Option<ErrorCode> {
    result.err().and_then(|e| e.code())
}

// ---------- 服务器核对 Cindy 凭证（与 HHH 同一套规则） ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cindy_tokens_are_checked_and_never_stored() {
    let env = Env::start().await;
    let url = env.server.url();
    let login = |token: String| {
        let url = url.clone();
        async move { Peer::connect(&url, "电脑", Credentials::generate(), Some(token), None).await }
    };
    // 两个签发方下同一个 membership 编号是两个账号；同一个身份每次都是同一个账号。
    let token_a = env.cindy.token("m1", "甲");
    let a = login(token_a.clone()).await.unwrap();
    let b = login(env.cindy.token_for(ISSUER_B, "m1", "甲")).await.unwrap();
    let again = login(env.cindy.token("m1", "甲")).await.unwrap();
    assert_ne!(a.client.uid(), b.client.uid());
    assert_eq!(a.client.uid(), again.client.uid());
    // 组织身份带组织编号可以登录。
    let now = unix_now();
    let base = json!({ "iss": ISSUER_A, "aud": "cindy", "sub": "m2", "typ": "access", "iat": now, "exp": now + 3600, "ctx": "personal", "displayName": "乙" });
    let with = |key: &str, value: serde_json::Value| {
        let mut claims = base.clone();
        claims[key] = value;
        claims
    };
    let mut org = with("ctx", json!("org"));
    org["org"] = json!("org_1");
    login(env.cindy.sign(org)).await.unwrap();

    let mut expired = with("iat", json!(now - 3700));
    expired["exp"] = json!(now - 400);
    let cases = [
        ("已过期", expired, ErrorCode::Expired),
        ("audience 不对", with("aud", json!("other")), ErrorCode::Unauthorized),
        ("不是 access", with("typ", json!("refresh")), ErrorCode::Unauthorized),
        ("签发方不认识", with("iss", json!("https://evil.test")), ErrorCode::Unauthorized),
        ("有效期超过 1 小时", with("exp", json!(now + 7200)), ErrorCode::Unauthorized),
        ("组织身份缺组织", with("ctx", json!("org")), ErrorCode::Unauthorized),
        ("membership 编号不合规", with("sub", json!("a/b")), ErrorCode::Unauthorized),
    ];
    for (what, claims, expected) in cases {
        let err = login(env.cindy.sign(claims)).await.err().unwrap();
        assert_eq!(code(&err), Some(expected), "{what}");
    }
    // 不认识的钥匙、被改过的签名。
    assert_eq!(code_of(login(env.cindy.sign_with_kid(base.clone(), "other-key")).await), Some(ErrorCode::Unauthorized));
    let token = env.cindy.sign(base);
    let mut tampered = token.clone().into_bytes();
    let at = tampered.len() - 10;
    tampered[at] = if tampered[at] == b'A' { b'B' } else { b'A' };
    assert_eq!(code_of(login(String::from_utf8(tampered).unwrap()).await), Some(ErrorCode::Unauthorized));

    // 库里只有登录凭证的摘要，没有 Cindy 凭证、也没有登录凭证本身。
    let session = a.client.credentials().await.session.unwrap();
    let dump = dump_server_files(&env.server);
    assert!(!contains(&dump, token_a.as_bytes()));
    assert!(!contains(&dump, &session.0));
    assert!(!contains(&dump, bytes::encode(&session.0).as_bytes()));
}

// ---------- 契约 §5：服务器拒绝越权与伪造；服务器私自改数据，插件识破 ----------

fn last_conv_link(db: &rusqlite::Connection, conv: &ConvId) -> (u64, ConvLink) {
    let (seq, link): (i64, String) = db
        .query_row("SELECT seq, link FROM conv_links WHERE conv = ?1 ORDER BY seq DESC LIMIT 1", params![conv.as_str()], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    (seq as u64, serde_json::from_str(&link).unwrap())
}

fn next_conv_body(db: &rusqlite::Connection, conv: &ConvId, author: &MemberRef, change: ConvChange) -> ConvLinkBody {
    let (seq, last) = last_conv_link(db, conv);
    ConvLinkBody {
        v: conv::LINK_VERSION,
        conv: conv.clone(),
        seq: seq + 1,
        prev: Some(last.hash()),
        ctime_ms: now_ms(),
        author: author.uid.clone(),
        author_eldest: author.eldest,
        change,
    }
}

fn count(db: &rusqlite::Connection, sql: &str, key: &str) -> i64 {
    db.query_row(sql, params![key], |r| r.get(0)).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forged_members_devices_and_keys_are_rejected() {
    // 最后测发送限速，用正式上限。
    let env = Env::with_production_limits().await;
    let (mut alice, _) = env.account("alice", "alice 的电脑").await;
    let (mut bob, _) = env.account("bob", "bob 的电脑").await;
    let (mallory, _) = env.account("mallory", "mallory 的电脑").await;
    let dm = approved_dm(&bob.client, &alice.client).await;
    let group = alice.client.create_group(&GroupProfile { name: "小组".into(), ..Default::default() }, &[bob.client.uid().clone()]).await.unwrap();
    let hello = send(&alice, &dm, "私聊内容").await;
    send(&alice, &group, "群内容").await;
    bob.wait_text("私聊内容").await;
    bob.wait_text("群内容").await;
    let (alice_ref, bob_ref, mallory_ref) = (me(&alice).await, me(&bob).await, me(&mallory).await);
    let alice_creds = alice.client.credentials().await;
    let bob_creds = bob.client.credentials().await;
    let mallory_creds = mallory.client.credentials().await;

    // 非成员读不到会话的任何东西。
    assert_eq!(code_of(mallory.client.conv_info(&group).await), Some(ErrorCode::Forbidden));
    assert!(mallory.client.invite(&group, std::slice::from_ref(&mallory_ref.uid)).await.is_err());

    // 绕过客户端直接发请求：非成员把自己签进群——服务器拒绝。
    let db = env.server.db();
    let (mut raw_mallory, _) = Raw::connect(&env.server.url(), &mallory_creds, "mallory 的电脑").await;
    let body = next_conv_body(&db, &group, &mallory_ref, ConvChange::Add { members: vec![mallory_ref.clone()] });
    let forged_add = ConvLink::sign(&body, &mallory_creds.device);
    let err = raw_mallory
        .request(Request::AppendConv { conv: group.clone(), links: vec![forged_add], keys: None, invite: None })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::Forbidden);

    // 转交：只能用自己这台电脑发；发给别的账号要说明会话、双方都是现在的成员。冒充别的设备、发给非成员一律被拒。
    let (mut raw_alice, _) = Raw::connect(&env.server.url(), &alice_creds, "alice 的电脑").await;
    // bob 本人也开着，转交给它的帧都送到。
    let (_raw_bob, _) = Raw::connect(&env.server.url(), &bob_creds, "bob 的电脑").await;
    let (alice_device, bob_device, mallory_device) = (alice_creds.device.id(), bob_creds.device.id(), mallory_creds.device.id());
    let relay = |to: &friend_im_client::core::ids::DeviceId, conv: Option<&ConvId>, frame: SealedFrame| Request::Relay { to: to.clone(), conv: conv.cloned(), frame };
    let as_alice = SealedFrame::seal(&alice_ref.uid, &alice_creds.device, &bob_creds.device.public(), b"x").unwrap();
    assert_eq!(raw_mallory.request(relay(&bob_device, Some(&dm), as_alice.clone())).await.unwrap_err().code, ErrorCode::Forbidden, "冒充别的设备");
    let from_mallory = SealedFrame::seal(&mallory_ref.uid, &mallory_creds.device, &bob_creds.device.public(), b"x").unwrap();
    assert_eq!(raw_mallory.request(relay(&bob_device, Some(&dm), from_mallory.clone())).await.unwrap_err().code, ErrorCode::Forbidden, "不是会话成员");
    assert_eq!(raw_mallory.request(relay(&bob_device, None, from_mallory)).await.unwrap_err().code, ErrorCode::Forbidden, "发给别的账号要说明会话");
    let to_mallory = SealedFrame::seal(&alice_ref.uid, &alice_creds.device, &mallory_creds.device.public(), b"x").unwrap();
    assert_eq!(raw_alice.request(relay(&mallory_device, Some(&dm), to_mallory)).await.unwrap_err().code, ErrorCode::Forbidden, "发给非成员");
    assert_eq!(raw_alice.request(relay(&bob_device, Some(&group), as_alice.clone())).await.unwrap(), Response::Relayed { online: true });
    // 帧里写的接收设备和要转给的不是同一台：拒绝。
    assert_eq!(raw_alice.request(relay(&alice_device, None, as_alice)).await.unwrap_err().code, ErrorCode::Forbidden);
    // 在线设备：只列能对账的。
    let Response::Peers { peers } = raw_mallory.request(Request::Peers).await.unwrap() else { panic!("在线设备回执") };
    assert!(peers.is_empty(), "没有共享会话的设备不算对账伙伴：{peers:?}");
    let Response::Peers { peers } = raw_alice.request(Request::Peers).await.unwrap() else { panic!("在线设备回执") };
    assert_eq!(peers.into_iter().map(|p| p.device).collect::<Vec<_>>(), [bob_device.clone()]);

    // 整批不生效：一个请求里第一环合规、第二环（群主移出却不换代）不合规，两环都不写入。
    let links_before = count(&db, "SELECT COUNT(*) FROM conv_links WHERE conv = ?1", group.as_str());
    let first = next_conv_body(&db, &group, &alice_ref, ConvChange::SetProfile { profile: Bytes(vec![1; 64]), generation: 1 });
    let first_link = ConvLink::sign(&first, &alice_creds.device);
    let second = ConvLinkBody {
        seq: first.seq + 1,
        prev: Some(first_link.hash()),
        change: ConvChange::Remove { uid: bob_ref.uid.clone(), reason: RemoveReason::Kicked },
        ..first.clone()
    };
    let second_link = ConvLink::sign(&second, &alice_creds.device);
    let err = raw_alice
        .request(Request::AppendConv { conv: group.clone(), links: vec![first_link, second_link], keys: None, invite: None })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::Invalid);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM conv_links WHERE conv = ?1", group.as_str()), links_before);
    // 换恢复密钥却没封装给新恢复密钥：整批不生效，账号签名链与恢复密钥都不变。
    let Response::Chain { entries, .. } = raw_alice.request(Request::GetChain { uid: alice_ref.uid.clone(), epoch: None, after: 0 }).await.unwrap() else {
        panic!("签名链回执");
    };
    let chain: Vec<(AccountLink, u64)> = entries.into_iter().map(|e| (e.link, e.log)).collect();
    let state = AccountState::verify_chain(&alice_ref.uid, 0, &chain).unwrap();
    let new_recovery = RecoveryKey::generate().device_secret();
    let next_seed = AccountSeed::generate();
    let generation = state.current_key().generation + 1;
    let change = AccountChange::ReplaceRecovery {
        recovery: new_recovery.public(),
        proof: device_proof(&new_recovery, &alice_ref.uid, 0, ProofRole::Recovery),
        key: next_seed.public(generation),
    };
    let link = AccountLink::sign(&state.next_body(change, now_ms()), &alice_creds.device);
    let to_me = alice_creds.device.public();
    let sealed = crypto::seal_to(&to_me.enc.0, &account_seed_info(&alice_ref.uid, 0, generation, &to_me.id()), next_seed.as_bytes()).unwrap();
    let bundle = AccountKeyBundle { generation, boxes: vec![DeviceKeyBox { device: to_me.id(), sealed }], prev: Some(Bytes(vec![0; 72])) };
    let account_links = count(&db, "SELECT COUNT(*) FROM account_links WHERE uid = ?1", alice_ref.uid.as_str());
    assert_eq!(raw_alice.request(Request::AppendAccount { links: vec![link], keys: Some(bundle) }).await.unwrap_err().code, ErrorCode::Invalid);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM account_links WHERE uid = ?1", alice_ref.uid.as_str()), account_links);

    // 服务器私自往群里加人（直接改库）：成员的插件验签名链时发现并拒绝。
    let (seq, _) = last_conv_link(&db, &group);
    let body = next_conv_body(&db, &group, &mallory_ref, ConvChange::Add { members: vec![mallory_ref.clone()] });
    db.execute(
        "INSERT INTO conv_links(conv, seq, link, log) VALUES(?1, ?2, ?3, ?4)",
        params![group.as_str(), (seq + 1) as i64, serde_json::to_string(&ConvLink::sign(&body, &mallory_creds.device)).unwrap(), 1_000_000i64],
    )
    .unwrap();
    let err = alice.client.rotate(&group).await.unwrap_err();
    assert!(matches!(err, ClientError::Verify(_)), "应识破服务器加的成员：{err}");
    db.execute("DELETE FROM conv_links WHERE conv = ?1 AND seq = ?2", params![group.as_str(), (seq + 1) as i64]).unwrap();

    // 服务器私自给 alice 加电脑：别人验 alice 的签名链时发现，她的消息不当成正常消息。
    let (account_seq, last): (i64, String) = db
        .query_row("SELECT seq, link FROM account_links WHERE uid = ?1 ORDER BY seq DESC LIMIT 1", params![alice_ref.uid.as_str()], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    let last: AccountLink = serde_json::from_str(&last).unwrap();
    let planted = DeviceSecret::generate();
    let body = AccountLinkBody {
        v: account::LINK_VERSION,
        uid: alice_ref.uid.clone(),
        epoch: 0,
        seq: account_seq as u64 + 1,
        prev: Some(last.hash()),
        ctime_ms: now_ms(),
        change: AccountChange::AddDevice {
            device: DeviceEntry { keys: planted.public(), name: "服务器塞的电脑".into(), kind: friend_im_core::wire::DeviceKind::Computer },
            proof: device_proof(&planted, &alice_ref.uid, 0, ProofRole::Device),
        },
    };
    db.execute(
        "INSERT INTO account_links(uid, epoch, seq, link, log) VALUES(?1, 0, ?2, ?3, ?4)",
        params![alice_ref.uid.as_str(), account_seq + 1, serde_json::to_string(&AccountLink::sign(&body, &planted)).unwrap(), 1_000_001i64],
    )
    .unwrap();
    let fresh_bob = Peer::connect(&env.server.url(), "bob 的电脑", bob_creds.clone(), None, None).await.unwrap();
    // Alice now signs the creation when approving. Tampering with her account
    // rejects the conversation chain before any message can be exposed.
    assert!(matches!(fresh_bob.client.notices(&dm).await.unwrap_err(), ClientError::Verify(_)));
    fresh_bob.client.close();
    db.execute("DELETE FROM account_links WHERE uid = ?1 AND seq = ?2", params![alice_ref.uid.as_str(), account_seq + 1]).unwrap();

    // 服务器塞来一把自己选的会话钥匙：和会话签名链里的承诺对不上，插件拒绝。
    let eldest_text: String = db
        .query_row("SELECT link FROM account_links WHERE uid = ?1 AND epoch = 0 AND seq = 1", params![bob_ref.uid.as_str()], |r| r.get(0))
        .unwrap();
    let eldest: AccountLink = serde_json::from_str(&eldest_text).unwrap();
    let AccountChange::Eldest { key, .. } = eldest.parse().unwrap().change else { panic!("第一环") };
    let original: String = db
        .query_row("SELECT sealed FROM conv_keys WHERE conv = ?1 AND generation = 1 AND uid = ?2", params![dm.as_str(), bob_ref.uid.as_str()], |r| r.get(0))
        .unwrap();
    let fake = crypto::seal_to(&key.enc.0, &conv_seed_info(&dm, 1, &bob_ref.uid, 0, 1), ConvSeed::generate().as_bytes()).unwrap();
    db.execute(
        "UPDATE conv_keys SET sealed = ?1 WHERE conv = ?2 AND generation = 1 AND uid = ?3",
        params![serde_json::to_string(&fake).unwrap(), dm.as_str(), bob_ref.uid.as_str()],
    )
    .unwrap();
    let fresh_bob = Peer::connect(&env.server.url(), "bob 的电脑", bob_creds.clone(), None, None).await.unwrap();
    let original_record = alice.host.db.lock().unwrap()[&dm][&hello].raw.clone();
    let err = fresh_bob.client.open_record(&dm, &original_record).await.unwrap_err();
    assert!(matches!(err, ClientError::Verify(_) | ClientError::Core(_)), "假钥匙不能解出任何东西：{err}");
    fresh_bob.client.close();
    db.execute(
        "UPDATE conv_keys SET sealed = ?1 WHERE conv = ?2 AND generation = 1 AND uid = ?3",
        params![original, dm.as_str(), bob_ref.uid.as_str()],
    )
    .unwrap();

    // 改回来之后一切照常。
    send(&bob, &dm, "一切恢复正常").await;
    alice.wait_text("一切恢复正常").await;

    // 上限：转交太快被拒；消息太大在本机就拦住。
    let mut limited = false;
    for _ in 0..limits::RELAY_PER_SECOND * 2 {
        let frame = SealedFrame::seal(&alice_ref.uid, &alice_creds.device, &bob_creds.device.public(), b"x").unwrap();
        if let Err(error) = raw_alice.request(relay(&bob_device, Some(&dm), frame)).await {
            assert_eq!(error.code, ErrorCode::RateLimited, "{error}");
            limited = true;
            break;
        }
    }
    assert!(limited, "一秒内转交太多帧应被限速");
    let long = "长".repeat(30 * 1024);
    assert!(matches!(alice.send_as(&dm, &MsgId::random(), text(&long)).await.unwrap_err(), ClientError::Core(_)));
}

// ---------- friend-im v10：服务器的数据目录里没有任何消息、文件和已读，加密后的也没有 ----------

/// 服务器数据目录里全部文件的字节（数据库、WAL）。
fn dump_server_files(server: &Server) -> Vec<Vec<u8>> {
    fn walk(dir: &std::path::Path, out: &mut Vec<Vec<u8>>) {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if let Ok(data) = std::fs::read(&path) {
                out.push(data);
            }
        }
    }
    let mut out = Vec::new();
    walk(server.dir.path(), &mut out);
    out
}

fn contains(files: &[Vec<u8>], needle: &[u8]) -> bool {
    files.iter().any(|data| data.windows(needle.len()).any(|w| w == needle))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_server_never_stores_a_message_a_recall_or_a_read_position() {
    let env = Env::start().await;
    let (mut alice, _) = env.account("alice", "alice 的电脑").await;
    let (mut bob, _) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &alice.client).await;
    let probes = [
        "私聊明文探针-甲乙丙丁",
        "群聊明文探针-戊己庚辛",
        "群名明文探针-壬癸",
        "群简介明文探针-子丑寅",
        "改过的群名探针-卯辰巳",
        "撤回的消息探针-天地玄黄",
        "回复里的明文探针-宇宙洪荒",
    ];
    send(&alice, &dm, probes[0]).await;
    let group = alice
        .client
        .create_group(&GroupProfile { name: probes[2].into(), description: probes[3].into(), avatar: None }, &[bob.client.uid().clone()])
        .await
        .unwrap();
    send(&bob, &group, probes[1]).await;
    let recalled = send(&alice, &dm, probes[5]).await;
    alice.recall(&dm, &recalled).await.unwrap();
    alice
        .send_as(&dm, &MsgId::random(), Content::Post { parts: vec![Part::Text { text: probes[6].into() }], reply_to: Some(recalled) })
        .await
        .unwrap();
    bob.client.set_group_profile(&group, &GroupProfile { name: probes[4].into(), ..Default::default() }).await.unwrap();
    let last = bob.wait_text(probes[6]).await;
    alice.wait_text(probes[1]).await;
    wait_notice(&mut alice, &group, "改群名", |c| matches!(c, ConvNotice::ProfileChanged)).await;
    bob.mark_read(&dm, &last).await.unwrap();

    // 收到的人解得开（确实送到了），服务器的数据目录里什么都找不到：明文没有，加密后的信封也没有。
    let files = dump_server_files(&env.server);
    assert!(!files.is_empty(), "至少有数据库");
    for probe in probes {
        assert!(!contains(&files, probe.as_bytes()), "服务器上找到了明文：{probe}");
        assert!(!contains(&files, bytes::encode(probe.as_bytes()).as_bytes()), "服务器上找到了 base64 形式的明文：{probe}");
    }
    for row in alice.host.db.lock().unwrap().values().flat_map(|rows| rows.values()) {
        assert!(!contains(&files, row.raw.as_bytes()), "服务器上找到了消息原件");
        let start = row.raw.find("\"ct\":\"").map(|at| at + 6);
        if let Some(start) = start {
            let ct = &row.raw[start..start + 60.min(row.raw.len() - start)];
            assert!(!contains(&files, ct.as_bytes()), "服务器上找到了信封的密文");
        }
    }
    let db = env.server.db();
    for table in ["messages", "files", "reading_states"] {
        let exists: i64 = db.query_row("SELECT COUNT(*) FROM sqlite_master WHERE name = ?1", params![table], |r| r.get(0)).unwrap();
        assert_eq!(exists, 0, "不该有 {table} 这张表");
    }
    let kinds: i64 = db.query_row("SELECT COUNT(*) FROM log WHERE kind IN ('message', 'reading')", [], |r| r.get(0)).unwrap();
    assert_eq!(kinds, 0, "日志里没有消息和已读事件");
    assert!(!env.server.dir.path().join("files").exists(), "没有文件目录");
}

// ---------- friend-im v10 第 3 项：不设离线队列，等双方都有设备在线再送达 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_message_reaches_an_offline_friend_through_any_device_that_has_it() {
    let env = Env::start().await;
    let (mut desktop, recovery) = env.account("alice", "台式机").await;
    let (bob, _) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &desktop.client).await;
    send(&bob, &dm, "热身").await;
    desktop.wait_text("热身").await;
    let mut laptop = env.device("alice", "笔记本").await;
    laptop.client.join_with_recovery(&recovery.to_words()).await.unwrap();
    laptop.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    laptop.wait_text("热身").await;

    // bob 下线；alice 的台式机发一条，服务器不留。
    let bob_creds = bob.client.credentials().await;
    let bob_host = bob.host.clone();
    bob.client.close();
    send(&desktop, &dm, "bob 不在线时发的").await;
    laptop.wait_text("bob 不在线时发的").await;

    // 发送的那台也下线，只留另一台：bob 上线后照样收到。
    desktop.client.close();
    let mut back = Peer::connect_with(bob_host, &env.server.url(), "bob 的电脑", bob_creds, None, None).await.unwrap();
    back.wait_text("bob 不在线时发的").await;
    let _ = &mut laptop;
}

// ---------- 难点场景：两台同时离线、撤回比原消息先到、长期离线的设备回来 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn devices_that_were_apart_end_up_identical() {
    let env = Env::start().await;
    let direct = format!("http://{}", env.server.direct);
    let mut alice = Peer::connect(&direct, "alice 的电脑", Credentials::generate(), Some(env.cindy.token("alice", "alice")), None).await.unwrap();
    alice.client.provision().await.unwrap();
    let (mut bob, _) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &alice.client).await;
    let old = send(&bob, &dm, "断之前").await;
    alice.wait_text("断之前").await;

    env.server.proxy.cut();
    bob.wait("断开", |u| matches!(u, Update::Disconnected).then_some(())).await;
    // 两边同时各发各的；alice 回了一条。bob 在断网时撤回自己的一条。
    send(&alice, &dm, "alice 断网时发的").await;
    send(&bob, &dm, "bob 断网时发的").await;
    bob.recall(&dm, &old).await.unwrap();
    env.server.proxy.restore();
    bob.wait("重新连上", |u| matches!(u, Update::Connected { status: DeviceStatus::Active }).then_some(())).await;
    eventually("撤回到了 alice", async || alice.is_recalled(&dm, &old).then_some(())).await;
    eventually("两边一模一样", async || {
        let (a, b) = (alice.texts(&dm).await, bob.texts(&dm).await);
        (a == b && a.len() == 2).then_some(())
    })
    .await;
    // 撤回的不会复活：再对一轮账还是墓碑。
    bob.settle(Duration::from_millis(300)).await;
    assert!(bob.is_recalled(&dm, &old) && alice.is_recalled(&dm, &old));
}
