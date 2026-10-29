//! 用真实客户端、WebSocket、签名与 SQLite 验证阅读，不依赖线上账号。
mod common;

use common::*;
use friend_im_client::core::{
    conv::GroupProfile,
    ids::MsgId,
    reading::ReadingEnvelope,
    wire::{DeviceStatus, ErrorCode, Request, Response},
};
use friend_im_client::{ReadingMark, Update};
use rusqlite::params;

async fn send(peer: &Peer, conv: &friend_im_client::core::ids::ConvId, body: &str) -> MsgId {
    let id = MsgId::random();
    peer.client.send(conv, &id, text(body)).await.unwrap();
    id
}

fn writes(env: &Env) -> (i64, i64) {
    let db = env.server.db();
    (
        db.query_row("SELECT count(*) FROM log WHERE kind='reading'", [], |r| {
            r.get(0)
        })
        .unwrap(),
        db.query_row("SELECT count(*) FROM reading_states", [], |r| r.get(0))
            .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_boundaries_unknown_readers_and_historical_membership() {
    let env = Env::start().await;
    let (alice, _) = env.account("alice", "pc").await;
    let (mut bob, _) = env.account("bob", "pc").await;
    let (mut carol, _) = env.account("carol", "pc").await;
    alice
        .client
        .add_friend(&bob.client.friend_link(false).await.unwrap())
        .await
        .unwrap();
    alice
        .client
        .add_friend(&carol.client.friend_link(false).await.unwrap())
        .await
        .unwrap();
    let group = alice
        .client
        .create_group(
            &GroupProfile::default(),
            &[bob.client.uid().clone(), carol.client.uid().clone()],
        )
        .await
        .unwrap();
    let first = send(&alice, &group, "边界前").await;
    let second = send(&alice, &group, "边界后").await;
    let first_message = bob.wait_text("边界前").await;
    let second_message = bob.wait_text("边界后").await;
    carol.wait_text("边界后").await;
    assert!(first_message.log < second_message.log);
    assert!(alice.client.supports_reading().await);
    let receipts = alice
        .client
        .reading_receipts(&group, &[first.clone(), second.clone()])
        .await
        .unwrap();
    assert!(
        receipts
            .iter()
            .all(|r| !r.complete && r.readers.is_empty() && r.unreaders.is_empty())
    );
    assert_eq!(writes(&env), (0, 0), "只读查询绝不写回已读");
    assert!(
        bob.client
            .mark_read(&group, &first, Some(second_message.log))
            .await
            .is_err()
    );
    assert_eq!(writes(&env), (0, 0), "错误边界不落库");
    assert_eq!(
        bob.client
            .mark_read(&group, &first, Some(first_message.log))
            .await
            .unwrap(),
        ReadingMark::Applied
    );
    let receipts = alice
        .client
        .reading_receipts(&group, &[first.clone(), second.clone()])
        .await
        .unwrap();
    assert_eq!(receipts[0].readers, [bob.client.uid().clone()]);
    assert_eq!(receipts[1].unreaders, [bob.client.uid().clone()]);
    assert!(!receipts[1].complete, "没写过阅读的旧客户端保持未知");
    assert_eq!(
        carol.client.mark_read(&group, &second, None).await.unwrap(),
        ReadingMark::Applied
    );
    carol.client.leave(&group).await.unwrap();
    let receipts = alice
        .client
        .reading_receipts(&group, std::slice::from_ref(&second))
        .await
        .unwrap();
    assert!(receipts[0].complete);
    assert_eq!(receipts[0].readers, [carol.client.uid().clone()]);
    assert_eq!(
        receipts[0].unreaders,
        [bob.client.uid().clone()],
        "退出成员仍属于当时的收件人"
    );
    let before = writes(&env);
    assert_eq!(
        bob.client.mark_read(&group, &first, None).await.unwrap(),
        ReadingMark::Applied
    );
    assert_eq!(writes(&env), before, "重复或更旧的边界不写、不重推");
    assert!(
        bob.client
            .reading_receipts(&group, &[second])
            .await
            .unwrap()
            .is_empty(),
        "不泄漏别人发的消息回执"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_device_restart_and_cas_preserve_the_latest_boundary() {
    let mut env = Env::start().await;
    let (alice, _) = env.account("alice", "pc").await;
    let (mut bob, _) = env.account("bob", "pc").await;
    let conv = alice
        .client
        .add_friend(&bob.client.friend_link(false).await.unwrap())
        .await
        .unwrap();
    let one = send(&alice, &conv, "第一条").await;
    let two = send(&alice, &conv, "第二条").await;
    let third = send(&alice, &conv, "第三条").await;
    let first = bob.wait_text("第一条").await;
    let second = bob.wait_text("第二条").await;
    bob.wait_text("第三条").await;
    let mut laptop = env.device("bob", "laptop").await;
    laptop
        .wait("待批准", |u| {
            matches!(
                u,
                Update::Connected {
                    status: DeviceStatus::Pending
                }
            )
            .then_some(())
        })
        .await;
    let join = laptop.client.join_start().await.unwrap();
    let request = bob
        .wait("批准申请", |u| {
            if let Update::JoinRequested(info) = u {
                Some(info.clone())
            } else {
                None
            }
        })
        .await;
    bob.client
        .join_respond(&request.request, join.code.as_str())
        .await
        .unwrap();
    laptop
        .wait("电脑可用", |u| {
            matches!(u, Update::Activated).then_some(())
        })
        .await;
    assert_eq!(
        bob.client
            .mark_read(&conv, &one, Some(first.log))
            .await
            .unwrap(),
        ReadingMark::Applied
    );
    let old_json: String = env
        .server
        .db()
        .query_row(
            "SELECT envelope FROM reading_states WHERE reader=?1",
            [bob.client.uid().as_str()],
            |r| r.get(0),
        )
        .unwrap();
    let old: ReadingEnvelope = serde_json::from_str(&old_json).unwrap();
    let creds = bob.client.credentials().await;
    let (mut raw, _) = Raw::connect(&env.server.url(), &creds, "协议核对").await;
    let before = writes(&env);
    raw.request(Request::ReadingSet {
        envelope: old.clone(),
    })
    .await
    .unwrap();
    assert_eq!(writes(&env), before, "完全相同的重放幂等");
    assert_eq!(
        bob.client
            .mark_read(&conv, &two, Some(second.log))
            .await
            .unwrap(),
        ReadingMark::Applied
    );
    assert_eq!(
        raw.request(Request::ReadingSet {
            envelope: old.clone()
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::Conflict
    );
    let mut tampered = old;
    tampered.ct.0[0] ^= 1;
    assert_eq!(
        raw.request(Request::ReadingSet { envelope: tampered })
            .await
            .unwrap_err()
            .code,
        ErrorCode::Invalid
    );
    assert_eq!(
        laptop.client.reading_through(&conv).await.unwrap(),
        Some(second.log)
    );
    let before = writes(&env);
    assert_eq!(
        laptop.client.mark_read(&conv, &one, None).await.unwrap(),
        ReadingMark::Applied
    );
    assert_eq!(writes(&env), before, "另一设备不能把已读倒退");
    let json: String = env
        .server
        .db()
        .query_row(
            "SELECT envelope FROM reading_states WHERE reader=?1",
            [bob.client.uid().as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        !json.contains(one.as_str()) && !json.contains(two.as_str()) && !json.contains("through"),
        "服务端不保存阅读消息或水位明文"
    );
    env.server.restart().await;
    bob.wait("断线", |u| {
        matches!(u, Update::Disconnected).then_some(())
    })
    .await;
    bob.wait("重连", |u| {
        matches!(
            u,
            Update::Connected {
                status: DeviceStatus::Active
            }
        )
        .then_some(())
    })
    .await;
    assert_eq!(
        bob.client.reading_through(&conv).await.unwrap(),
        Some(second.log)
    );
    let receipt = alice
        .client
        .reading_receipts(&conv, &[third])
        .await
        .unwrap();
    assert_eq!(
        receipt[0].unreaders,
        [bob.client.uid().clone()],
        "重启不会读掉边界之后的消息"
    );
    // 数据被服务端改坏时，客户端不生成任何伪回执。
    let mut corrupt: ReadingEnvelope = serde_json::from_str(&json).unwrap();
    corrupt.ct.0[0] ^= 1;
    env.server
        .db()
        .execute(
            "UPDATE reading_states SET envelope=?1 WHERE reader=?2",
            params![
                serde_json::to_string(&corrupt).unwrap(),
                bob.client.uid().as_str()
            ],
        )
        .unwrap();
    assert!(alice.client.reading_receipts(&conv, &[one]).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reading_rejects_foreign_conversations_and_pending_devices() {
    let env = Env::start().await;
    let (alice, _) = env.account("alice", "pc").await;
    let (mut bob, _) = env.account("bob", "pc").await;
    let (eve, _) = env.account("eve", "pc").await;
    let conv = alice
        .client
        .add_friend(&bob.client.friend_link(false).await.unwrap())
        .await
        .unwrap();
    let id = send(&alice, &conv, "不能跨会话").await;
    bob.wait_text("不能跨会话").await;
    assert_eq!(
        code(&eve.client.reading_through(&conv).await.unwrap_err()),
        Some(ErrorCode::Forbidden)
    );
    let pending = env.device("bob", "pending").await;
    let (mut raw, _) = Raw::connect(
        &env.server.url(),
        &pending.client.credentials().await,
        "pending",
    )
    .await;
    assert_eq!(
        raw.request(Request::ReadingGet {
            conv: conv.clone(),
            after: 0,
            limit: 20
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::NotApproved
    );
    let other = alice
        .client
        .add_friend(&eve.client.friend_link(false).await.unwrap())
        .await
        .unwrap();
    assert!(alice.client.mark_read(&other, &id, None).await.is_err());
    assert!(
        alice
            .client
            .reading_messages(&other, &[id])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(writes(&env), (0, 0));
    let (mut raw, _) = Raw::connect(
        &env.server.url(),
        &alice.client.credentials().await,
        "核对分页",
    )
    .await;
    assert!(matches!(
        raw.request(Request::ReadingGet {
            conv: conv.clone(),
            after: 0,
            limit: 200
        })
        .await
        .unwrap(),
        Response::ReadingPage { more: false, .. }
    ));
    assert_eq!(
        raw.request(Request::ReadingGet {
            conv,
            after: u64::MAX,
            limit: 200
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::BadRequest
    );
}
