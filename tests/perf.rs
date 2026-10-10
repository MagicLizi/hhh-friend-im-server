//! 性能（契约 §7）：新电脑 1 万条历史 5 分钟内补齐、批准后 30 秒内出现会话列表、消息 2 秒内到对方。
//!
//! 灌 1 万条消息要放宽发送频率，数字按发布构建量才有意义，默认不跑：
//! `cargo test --release --test perf -- --ignored --nocapture`

mod common;

use std::time::{Duration, Instant};

use common::*;
use friend_im_client::core::ids::MsgId;
use friend_im_client::{HistoryItem, Update};

const MESSAGES: usize = 10_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "量性能：cargo test --release --test perf -- --ignored --nocapture"]
async fn new_computer_catches_up_ten_thousand_messages() {
    let env = Env::start_with(|c| {
        c.send_per_second = 100_000;
        c.send_per_minute = 1_000_000;
    })
    .await;
    let (mut alice, recovery) = env.account("alice", "台式机").await;
    let (mut bob, _) = env.account("bob", "bob 的电脑").await;
    let dm = bob.client.add_friend(&alice.client.friend_link(false).await.unwrap()).await.unwrap();

    // 两台都在线时的单条延迟：点发送到对方收到。
    let mut worst = Duration::ZERO;
    for i in 0..20 {
        let started = Instant::now();
        alice.client.send(&dm, &MsgId::random(), text(&format!("延迟 {i}"))).await.unwrap();
        bob.wait_text(&format!("延迟 {i}")).await;
        worst = worst.max(started.elapsed());
    }
    println!("消息到对方：20 条里最慢 {worst:?}");
    assert!(worst < Duration::from_secs(2));

    // 灌 1 万条（bob 发，alice 的台式机在线照常收）。
    let started = Instant::now();
    for i in 0..MESSAGES {
        bob.client.send(&dm, &MsgId::random(), text(&format!("历史 {i}"))).await.unwrap();
    }
    println!("发 {MESSAGES} 条用了 {:?}", started.elapsed());
    alice.wait_text(&format!("历史 {}", MESSAGES - 1)).await;

    // 新电脑用恢复密钥加入，量会话列表出现、全部历史补齐的时间。
    let mut laptop = env.device("alice", "笔记本").await;
    let joined = Instant::now();
    laptop.client.join_with_recovery(&recovery.to_words()).await.unwrap();
    laptop.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    let convs = laptop.client.convs().await.unwrap();
    assert!(convs.iter().any(|c| c.conv == dm));
    let listed = joined.elapsed();
    let mut before = None;
    let mut messages = 0;
    let mut pages = 0;
    loop {
        let page = laptop.client.history(&dm, before, 100).await.unwrap();
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
    println!("新电脑：会话列表 {listed:?}；{pages} 页、{messages} 条历史全部补齐 {all:?}");
    assert_eq!(messages, MESSAGES + 20);
    assert!(listed < Duration::from_secs(30));
    assert!(all < Duration::from_secs(300));

    // 补齐期间收发新消息不受影响。
    let started = Instant::now();
    bob.client.send(&dm, &MsgId::random(), text("补齐之后")).await.unwrap();
    laptop.wait_text("补齐之后").await;
    println!("补齐后新消息到新电脑：{:?}", started.elapsed());
}
