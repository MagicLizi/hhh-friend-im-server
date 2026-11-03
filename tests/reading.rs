//! 已读记录（friend-im v10 第 5 项）：跟着设备对账走，服务器不存；每台设备取最新的一份，所有设备算出来一样。
mod common;

use std::time::{Duration, Instant};

use common::*;
use friend_im_client::core::conv::GroupProfile;
use friend_im_client::core::ids::{ConvId, MsgId};
use friend_im_client::core::sync::Key;
use friend_im_client::Update;

async fn send(peer: &Peer, conv: &ConvId, body: &str) -> MsgId {
    let id = MsgId::random();
    peer.send_as(conv, &id, text(body)).await.unwrap();
    id
}

async fn through(peer: &Peer, conv: &ConvId) -> Option<Key> {
    peer.client.reading_through(conv).await.unwrap()
}

async fn wait_through(peer: &Peer, conv: &ConvId, want: &Key) {
    let deadline = Instant::now() + WAIT;
    loop {
        if through(peer, conv).await.as_ref() == Some(want) {
            return;
        }
        assert!(Instant::now() < deadline, "{} 等不到已读位置", peer.name);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_positions_travel_between_devices_and_stay_the_same_everywhere() {
    let env = Env::start().await;
    let (mut desktop, recovery) = env.account("alice", "台式机").await;
    let (bob, _) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &desktop.client).await;
    send(&bob, &dm, "第一条").await;
    send(&bob, &dm, "第二条").await;
    let first = desktop.wait_text("第一条").await;
    let second = desktop.wait_text("第二条").await;
    let mut laptop = env.device("alice", "笔记本").await;
    laptop.client.join_with_recovery(&recovery.to_words()).await.unwrap();
    laptop.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    laptop.wait_text("第二条").await;

    // 台式机读到第二条：笔记本跟着知道，且只认最后读到的位置。
    desktop.mark_read(&dm, &second).await.unwrap();
    let key = Key { at: second.ctime_ms, id: second.id.clone() };
    wait_through(&laptop, &dm, &key).await;

    // 乱序上报：笔记本后来才报读到第一条，不会让位置往回退。
    laptop.mark_read(&dm, &first).await.unwrap();
    assert_eq!(through(&laptop, &dm).await, Some(key.clone()));
    assert_eq!(through(&desktop, &dm).await, Some(key.clone()));

    // 笔记本重启：内存里的记录没了，从在线的台式机补回来。
    let creds = laptop.client.credentials().await;
    let host = laptop.host.clone();
    laptop.client.close();
    let reopened = Peer::connect_with(host, &env.server.url(), "笔记本", creds, None, None).await.unwrap();
    wait_through(&reopened, &dm, &key).await;

    // 服务器上什么都没有。
    let tables: i64 = env.server.db().query_row("SELECT COUNT(*) FROM sqlite_master WHERE name = 'reading_states'", [], |r| r.get(0)).unwrap();
    assert_eq!(tables, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receipts_only_count_who_was_there_when_the_message_was_sent() {
    let env = Env::start().await;
    let (mut alice, _) = env.account("alice", "alice 的电脑").await;
    let (mut bob, _) = env.account("bob", "bob 的电脑").await;
    let (mut carol, _) = env.account("carol", "carol 的电脑").await;
    let (dave, _) = env.account("dave", "dave 的电脑").await;
    approved_dm(&bob.client, &alice.client).await;
    approved_dm(&carol.client, &alice.client).await;
    approved_dm(&dave.client, &alice.client).await;
    let group = alice
        .client
        .create_group(&GroupProfile { name: "回执".into(), ..Default::default() }, &[bob.client.uid().clone(), carol.client.uid().clone()])
        .await
        .unwrap();
    send(&alice, &group, "dave 进群之前").await;
    let message = alice.wait_text("dave 进群之前").await;
    let seen_by_bob = bob.wait_text("dave 进群之前").await;
    carol.wait_text("dave 进群之前").await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    alice.client.invite(&group, &[dave.client.uid().clone()]).await.unwrap();

    // 还没有 carol 的记录：名单不完整，不把她算成未读。
    bob.mark_read(&group, &seen_by_bob).await.unwrap();
    let uid = bob.client.uid().clone();
    let deadline = Instant::now() + WAIT;
    let receipt = loop {
        let receipts = alice.client.reading_receipts(&group, std::slice::from_ref(&message)).await.unwrap();
        if receipts[0].readers.contains(&uid) {
            break receipts.into_iter().next().unwrap();
        }
        assert!(Instant::now() < deadline, "等不到 bob 的已读");
        tokio::time::sleep(Duration::from_millis(30)).await;
    };
    assert!(!receipt.complete, "carol 的情况未知");
    assert!(!receipt.readers.contains(dave.client.uid()) && !receipt.unreaders.contains(dave.client.uid()), "dave 发消息时不在群里");
}
