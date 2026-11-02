//! 性能（friend-im v10）：消息 2 秒内到对方；新电脑从开着的旧电脑补 1 万条历史 5 分钟内，补齐期间收发不受影响。
//!
//! 灌 1 万条要放宽转交频率，数字按发布构建量才有意义，默认不跑：
//! `cargo test --release --test perf -- --ignored --nocapture`

mod common;

use std::time::{Duration, Instant};

use common::*;
use friend_im_client::core::ids::MsgId;
use friend_im_client::Update;

const MESSAGES: usize = 10_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "量性能：cargo test --release --test perf -- --ignored --nocapture"]
async fn new_computer_catches_up_ten_thousand_messages() {
    let env = Env::start_with(|c| {
        c.relay_per_second = 100_000;
        c.relay_per_minute = 1_000_000;
    })
    .await;
    let (alice, recovery) = env.account("alice", "台式机").await;
    let (mut bob, _) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &alice.client).await;

    // 两台都在线时的单条延迟：点发送到对方收到。
    let mut worst = Duration::ZERO;
    for i in 0..20 {
        let started = Instant::now();
        alice.send_as(&dm, &MsgId::random(), text(&format!("延迟 {i}"))).await.unwrap();
        bob.wait_text(&format!("延迟 {i}")).await;
        worst = worst.max(started.elapsed());
    }
    println!("消息到对方：20 条里最慢 {worst:?}");
    assert!(worst < Duration::from_secs(2));

    // 灌 1 万条（bob 发，alice 的台式机在线照常收）。
    let started = Instant::now();
    for i in 0..MESSAGES {
        bob.send_as(&dm, &MsgId::random(), text(&format!("历史 {i}"))).await.unwrap();
    }
    println!("发 {MESSAGES} 条用了 {:?}", started.elapsed());
    let count = |peer: &Peer| peer.host.db.lock().unwrap().get(&dm).map_or(0, |rows| rows.len());
    let deadline = Instant::now() + Duration::from_secs(300);
    while count(&alice) < MESSAGES + 20 {
        assert!(Instant::now() < deadline, "alice 收不齐：{}", count(&alice));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 新电脑用恢复密钥加入，量会话列表出现、全部历史补齐的时间。
    let mut laptop = env.device("alice", "笔记本").await;
    let joined = Instant::now();
    laptop.client.join_with_recovery(&recovery.to_words()).await.unwrap();
    laptop.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    let convs = laptop.client.convs().await.unwrap();
    assert!(convs.iter().any(|c| c.conv == dm));
    let listed = joined.elapsed();

    // 补齐途中发一条，看是不是受影响。
    tokio::time::sleep(Duration::from_millis(200)).await;
    let during = Instant::now();
    bob.send_as(&dm, &MsgId::random(), text("补齐途中")).await.unwrap();
    laptop.wait_text("补齐途中").await;
    println!("补齐途中的新消息到新电脑：{:?}（此时已补 {} 条）", during.elapsed(), count(&laptop));
    assert!(during.elapsed() < Duration::from_secs(10));

    while count(&laptop) < MESSAGES + 21 {
        assert!(joined.elapsed() < Duration::from_secs(300), "5 分钟内没补齐：{}", count(&laptop));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("新电脑：会话列表 {listed:?}；{} 条历史全部补齐 {:?}", count(&laptop), joined.elapsed());
    assert!(listed < Duration::from_secs(30));
}
