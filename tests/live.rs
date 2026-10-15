//! 线上验收：连已经部署好的服务器，用两个真实 Cindy 账号（比如个人身份和组织身份）走一遍主要流程，量真实网络下的数字。默认不跑。
//!
//! 会在服务器上开通这两个账号、留下测试电脑，所以只对还没人用的新服务器跑，跑完清空服务器数据（README「线上验收」）。
//! 凭证只从环境变量读，不落盘、不打印：
//!
//! `FRIEND_IM_LIVE_URL=https://im.example.com FRIEND_IM_LIVE_TOKEN_A=<凭证> FRIEND_IM_LIVE_TOKEN_B=<凭证> cargo test --release --test live -- --ignored --nocapture`
//!
//! - `FRIEND_IM_LIVE_HISTORY`：灌多少条历史再量新电脑补齐（默认 10000，契约 §7）；要服务器临时放宽发送频率，量完改回。设 0 跳过。
//! - `FRIEND_IM_LIVE_PROBES_OUT`：把这次的探针编号写进这个文件。所有消息、群名、文件名和文件内容都带着它，
//!   之后到服务器上查数据库、文件目录和日志里有没有它，就知道有没有明文落到服务器上。

mod common;

use std::io::Cursor;
use std::time::{Duration, Instant};

use common::*;
use friend_im_client::core::conv::{ConvKind, GroupProfile};
use friend_im_client::core::ids::{ConvId, MsgId};
use friend_im_client::core::message::{Content, Part};
use friend_im_client::core::wire::{DeviceStatus, ErrorCode};
use friend_im_client::{ConvNotice, Credentials, HistoryItem, InviteLink, Notice, Update};

/// 两台电脑之间发消息的间隔：正式的发送频率是每秒 5 条，量延迟时不撞上它。
const PACE: Duration = Duration::from_millis(250);

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("要设 {name}，见文件开头"))
}

async fn send(peer: &Peer, conv: &ConvId, body: &str) {
    peer.client.send(conv, &MsgId::random(), text(body)).await.unwrap_or_else(|e| panic!("{} 发「{body}」失败：{e}", peer.name));
}

async fn wait_notice(peer: &mut Peer, conv: &ConvId, what: &str, f: impl Fn(&ConvNotice) -> bool) -> Notice {
    peer.wait(what, |u| match u {
        Update::Conv(n) if &n.conv == conv && f(&n.change) => Some(n.clone()),
        _ => None,
    })
    .await
}

fn texts(items: &[HistoryItem]) -> Vec<String> {
    items
        .iter()
        .filter_map(|i| match i {
            HistoryItem::Message(m) => text_of(m),
            _ => None,
        })
        .collect()
}

/// 刚连上的电脑：账号必须还没开通（只对新服务器跑）。
async fn expect_unprovisioned(peer: &mut Peer) {
    let status = peer
        .wait("连上", |u| match u {
            Update::Connected { status } => Some(*status),
            _ => None,
        })
        .await;
    assert_eq!(status, DeviceStatus::Unprovisioned, "{} 的账号在服务器上已经开通过：只对新服务器跑，跑完清空数据", peer.name);
}

/// 一组延迟里的中位数和最慢。
fn spread(mut samples: Vec<Duration>) -> (Duration, Duration) {
    samples.sort();
    (samples[samples.len() / 2], samples[samples.len() - 1])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "线上验收：要真实 Cindy 凭证，见文件开头"]
async fn live_against_deployed_server() {
    init_logs();
    let url = env("FRIEND_IM_LIVE_URL");
    let token_a = env("FRIEND_IM_LIVE_TOKEN_A");
    let token_b = env("FRIEND_IM_LIVE_TOKEN_B");
    let history: usize = std::env::var("FRIEND_IM_LIVE_HISTORY").ok().map_or(10_000, |v| v.parse().expect("FRIEND_IM_LIVE_HISTORY 是条数"));
    // 探针编号：所有明文都带着它；电脑名服务器本来就看得到，不带。
    let probe = format!("{:016x}", rand::random::<u64>());
    if let Ok(path) = std::env::var("FRIEND_IM_LIVE_PROBES_OUT") {
        std::fs::write(path, format!("{probe}\n")).unwrap();
    }
    let mut report: Vec<(String, String)> = Vec::new();
    let mut note = |item: &str, value: String| {
        println!("  {item}：{value}");
        report.push((item.into(), value));
    };
    let connect = async |token: &str, device: &str| {
        let started = Instant::now();
        let peer = Peer::connect(&url, device, Credentials::generate(), Some(token.to_owned()), None)
            .await
            .unwrap_or_else(|e| panic!("{device} 登录失败：{e}"));
        (peer, started.elapsed())
    };

    // S1：两个账号第一台电脑开通（凭 Cindy 凭证自动开通，不填任何东西）。
    let (mut a1, took) = connect(&token_a, "R2 账号甲的台式机").await;
    note("连上并登录（wss + Cindy 凭证）", format!("{took:?}"));
    expect_unprovisioned(&mut a1).await;
    let started = Instant::now();
    let recovery = a1.client.provision().await.unwrap();
    note("开通账号甲", format!("{:?}", started.elapsed()));
    let (mut b1, _) = connect(&token_b, "R2 账号乙的电脑").await;
    expect_unprovisioned(&mut b1).await;
    let started = Instant::now();
    b1.client.provision().await.unwrap();
    note("开通账号乙", format!("{:?}", started.elapsed()));
    assert_ne!(a1.client.uid(), b1.client.uid(), "两个凭证是同一个 Cindy 身份");

    // S2：第二台电脑：新电脑显示 4 个词，在旧电脑上输入。
    let (mut a2, _) = connect(&token_a, "R2 账号甲的笔记本").await;
    a2.wait("连上（待批准）", |u| matches!(u, Update::Connected { status: DeviceStatus::Pending }).then_some(())).await;
    let words = a2.client.join_start().await.unwrap().code;
    let request = a1
        .wait("新电脑请求加入", |u| match u {
            Update::JoinRequested(info) => Some(info.clone()),
            _ => None,
        })
        .await;
    let started = Instant::now();
    a1.client.join_respond(&request.request, words.as_str()).await.unwrap();
    a2.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    a2.client.convs().await.unwrap();
    note("旧电脑输完 4 个词到新电脑可用、出现会话列表", format!("{:?}", started.elapsed()));

    // S8、S9：加好友、私聊；同账号的两台电脑都收到。
    let started = Instant::now();
    let dm = b1.client.add_friend(&a1.client.friend_link(false).await.unwrap()).await.unwrap();
    note("凭好友链接加好友", format!("{:?}", started.elapsed()));
    let first = format!("R2私聊明文探针-{probe}");
    send(&a1, &dm, &first).await;
    b1.wait_text(&first).await;
    a2.wait_text(&first).await;

    // 契约 §7：消息 2 秒内到对方；两个方向各 20 条。
    let mut there = Vec::new();
    let mut back = Vec::new();
    for i in 0..20 {
        let body = format!("延迟-{probe}-甲到乙-{i}");
        let started = Instant::now();
        send(&a1, &dm, &body).await;
        b1.wait_text(&body).await;
        there.push(started.elapsed());
        tokio::time::sleep(PACE).await;
        let body = format!("延迟-{probe}-乙到甲-{i}");
        let started = Instant::now();
        send(&b1, &dm, &body).await;
        a1.wait_text(&body).await;
        back.push(started.elapsed());
        tokio::time::sleep(PACE).await;
    }
    let (mid, worst) = spread(there.iter().chain(&back).copied().collect());
    note("点发送到对方收到（40 条）", format!("中位 {mid:?}，最慢 {worst:?}"));
    assert!(worst < Duration::from_secs(2), "契约 §7：2 秒内到对方");

    // S11：建群；群名和简介只有成员看得到。
    let profile = GroupProfile { name: format!("R2群名明文探针-{probe}"), description: format!("R2群简介明文探针-{probe}"), avatar: None };
    let started = Instant::now();
    let group = a1.client.create_group(&profile, &[b1.client.uid().clone()]).await.unwrap();
    for peer in [&mut b1, &mut a2] {
        wait_notice(peer, &group, "建群提示", |c| matches!(c, ConvNotice::Created { kind: ConvKind::Group, .. })).await;
    }
    note("建群到成员看到", format!("{:?}", started.elapsed()));
    assert_eq!(b1.client.group_profile(&group).await.unwrap(), Some(profile.clone()));
    let in_group = format!("R2群聊明文探针-{probe}");
    send(&b1, &group, &in_group).await;
    a1.wait_text(&in_group).await;
    a2.wait_text(&in_group).await;

    // 退群：退出的人读不到之后的；再凭群邀请链接回来，读得到新消息（S12）。
    let invite = a1.client.create_invite_link(&group).await.unwrap().to_string();
    b1.client.leave(&group).await.unwrap();
    wait_notice(&mut a1, &group, "退群提示", |c| matches!(c, ConvNotice::Left)).await;
    let while_away = format!("退群之后-{probe}");
    send(&a1, &group, &while_away).await;
    a2.wait_text(&while_away).await;
    b1.settle(Duration::from_secs(1)).await;
    assert!(!b1.messages().iter().any(|m| text_of(m).as_deref() == Some(while_away.as_str())), "退群后还收到群消息");
    assert!(b1.client.send(&group, &MsgId::random(), text("退群后还能发吗")).await.is_err());
    let started = Instant::now();
    b1.client.join_by_invite(&InviteLink::parse(&invite).unwrap()).await.unwrap();
    wait_notice(&mut a1, &group, "凭链接回来", |c| matches!(c, ConvNotice::JoinedByInvite)).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let page = b1.client.history(&group, None, 50).await.unwrap();
        if texts(&page.items).contains(&while_away) {
            break;
        }
        assert!(Instant::now() < deadline, "凭链接回来后 30 秒还读不到当前一代的消息");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    note("凭群邀请链接回到群、读到当前的消息", format!("{:?}", started.elapsed()));
    let back_again = format!("回来之后-{probe}");
    send(&a1, &group, &back_again).await;
    b1.wait_text(&back_again).await;

    // S5：3 MiB 文件；文件名和内容服务器都看不到。
    let unit = format!("R2文件内容明文探针-{probe}\n");
    let content: Vec<u8> = unit.as_bytes().iter().copied().cycle().take(3 * 1024 * 1024).collect();
    let started = Instant::now();
    let file = b1
        .client
        .upload(&dm, Cursor::new(content.clone()), content.len() as u64, &format!("R2文件名明文探针-{probe}.txt"), "text/plain")
        .await
        .unwrap();
    let up = started.elapsed();
    b1.client.send(&dm, &MsgId::random(), Content::Post { parts: vec![Part::File { file }], reply_to: None }).await.unwrap();
    let got = a1
        .wait("文件消息", |u| match u {
            Update::Message(m) => match &m.content {
                Some(Content::Post { parts, .. }) => parts.iter().find_map(|p| match p {
                    Part::File { file } => Some(file.clone()),
                    _ => None,
                }),
                _ => None,
            },
            _ => None,
        })
        .await;
    let started = Instant::now();
    let mut out = Vec::new();
    a1.client.download(&got, &mut out).await.unwrap();
    let down = started.elapsed();
    assert!(out == content, "下载的和上传的不一样");
    note("3 MiB 文件", format!("上传 {up:?}，下载 {down:?}"));

    // 契约 §7：新电脑凭恢复密钥加入，30 秒内出现会话列表，1 万条历史 5 分钟内补齐。
    // 甲的两台电脑先下线：这一段只量新电脑，不让在线电脑收推送占带宽。
    a1.client.close();
    a2.client.close();
    if history > 0 {
        // 一条发完再发下一条（和 perf 一样）：发送时客户端占着状态锁，自己这条的推送也要等这把锁才能处理；
        // 同时发多条会让推送积压到上限，客户端按设计断开重连，在路上的发送全按“没连上”失败。
        let started = Instant::now();
        for i in 0..history {
            if let Err(error) = b1.client.send(&dm, &MsgId::random(), text(&format!("历史-{probe}-{i}"))).await {
                assert_ne!(code(&error), Some(ErrorCode::RateLimited), "服务器没放宽发送频率：灌历史前临时调大，见文件开头");
                panic!("灌历史第 {i} 条失败：{error}");
            }
        }
        note(&format!("灌 {history} 条历史"), format!("{:?}", started.elapsed()));
    }
    let (mut a3, _) = connect(&token_a, "R2 账号甲的新电脑").await;
    let joined = Instant::now();
    a3.client.join_with_recovery(&recovery.to_words()).await.unwrap();
    a3.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    let convs = a3.client.convs().await.unwrap();
    assert!(convs.iter().any(|c| c.conv == dm) && convs.iter().any(|c| c.conv == group));
    let listed = joined.elapsed();
    let mut before = None;
    let (mut messages, mut pages) = (0, 0);
    loop {
        let page = a3.client.history(&dm, before, 100).await.unwrap();
        pages += 1;
        for item in &page.items {
            match item {
                HistoryItem::Message(_) => messages += 1,
                HistoryItem::Rejected { reason, .. } => panic!("历史里有解不开的：{reason}"),
                HistoryItem::Notice(_) => {}
            }
        }
        if !page.more {
            break;
        }
        before = page.items.iter().filter(|i| matches!(i, HistoryItem::Message(_))).map(HistoryItem::log).min();
    }
    let all = joined.elapsed();
    // 私聊里：探针 1 条 + 延迟 40 条 + 文件 1 条 + 灌的历史。
    assert_eq!(messages, history + 42, "补齐的条数不对");
    note("新电脑凭恢复密钥加入：出现会话列表", format!("{listed:?}"));
    note("新电脑补齐私聊全部历史", format!("{messages} 条、{pages} 页，{all:?}"));
    assert!(listed < Duration::from_secs(30), "契约 §7：30 秒内出现会话列表");
    assert!(all < Duration::from_secs(300), "契约 §7：1 万条 5 分钟内补齐");
    let started = Instant::now();
    let after = format!("补齐之后-{probe}");
    send(&b1, &dm, &after).await;
    a3.wait_text(&after).await;
    note("补齐后新消息到新电脑", format!("{:?}", started.elapsed()));

    println!("\nR2 线上验收通过（{url}），探针编号 {probe}：");
    for (item, value) in &report {
        println!("| {item} | {value} |");
    }
}
