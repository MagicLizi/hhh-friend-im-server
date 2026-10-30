mod common;
use common::*;
use friend_im_client::{
    Update,
    core::{
        friends::FriendRequestStatus as Status,
        ids::{MsgId, RequestId},
        wire::ErrorCode,
    },
};

#[tokio::test]
async fn approval_is_required_idempotent_private_and_survives_restart() {
    let mut env = Env::start().await;
    let (mut alice, _) = env.account("approval-alice", "A").await;
    let (mut bob, _) = env.account("approval-bob", "B").await;
    let (eve, _) = env.account("approval-eve", "E").await;
    let link = bob.client.friend_link(false).await.unwrap();
    assert_eq!(
        alice.client.add_friend(&link).await.unwrap_err().code(),
        Some(ErrorCode::Forbidden),
        "legacy direct creation must not bypass approval"
    );
    let id = RequestId::random();
    let sent = alice
        .client
        .request_link_friend(&id, &link, "你好，我是同群的 Alice")
        .await
        .unwrap();
    assert_eq!(
        sent.record.expires_ms - sent.record.created_ms,
        7 * 24 * 60 * 60 * 1000
    );
    let stored: String = env
        .server
        .db()
        .query_row("SELECT record FROM friend_requests", [], |r| r.get(0))
        .unwrap();
    assert!(!stored.contains("你好") && !stored.contains("Alice"));
    assert_eq!(sent.record.status, Status::Pending);
    assert!(alice.client.convs().await.unwrap().is_empty());
    assert!(bob.client.convs().await.unwrap().is_empty());
    assert_eq!(
        alice
            .client
            .request_link_friend(&id, &link, "你好，我是同群的 Alice")
            .await
            .unwrap()
            .record,
        sent.record,
        "lost response retry reuses the same record"
    );
    let incoming = bob.client.friend_requests(true, None, None).await.unwrap();
    assert_eq!(incoming.pending, 1);
    assert_eq!(
        incoming.requests[0].message.as_deref(),
        Some("你好，我是同群的 Alice")
    );
    assert!(
        eve.client
            .friend_requests(true, None, None)
            .await
            .unwrap()
            .requests
            .is_empty()
    );
    assert_eq!(
        eve.client.accept_friend(&id).await.unwrap_err().code(),
        Some(ErrorCode::NotFound)
    );
    assert!(alice.client.accept_friend(&id).await.is_err());
    let reverse = bob
        .client
        .request_link_friend(
            &RequestId::random(),
            &alice.client.friend_link(false).await.unwrap(),
            "你好",
        )
        .await
        .unwrap();
    assert_eq!(
        reverse.record.envelope.body.id, id,
        "crossed requests remain pending for explicit approval"
    );
    let accepted = bob.client.accept_friend(&id).await.unwrap();
    assert_eq!(accepted.record.status, Status::Accepted);
    let dm = accepted.record.conversation.clone().unwrap();
    assert_eq!(
        bob.client
            .accept_friend(&id)
            .await
            .unwrap()
            .record
            .conversation,
        Some(dm.clone())
    );
    assert_eq!(alice.client.convs().await.unwrap().len(), 1);
    alice
        .client
        .send(&dm, &MsgId::random(), text("批准后的加密消息"))
        .await
        .unwrap();
    bob.wait_text("批准后的加密消息").await;
    alice
        .wait("首次连接", |u| {
            matches!(u, Update::Connected { .. }).then_some(())
        })
        .await;
    env.server.restart().await;
    alice
        .wait("连接中断", |u| {
            matches!(u, Update::Disconnected).then_some(())
        })
        .await;
    alice
        .wait("重新连接", |u| {
            matches!(u, Update::Connected { .. }).then_some(())
        })
        .await;
    let restored = alice
        .client
        .friend_requests(false, None, None)
        .await
        .unwrap();
    assert_eq!(restored.requests[0].record.status, Status::Accepted);
    assert_eq!(restored.requests[0].record.conversation, Some(dm));
}

#[tokio::test]
async fn expiry_cooldown_atomic_failure_and_racing_devices_keep_one_outcome() {
    use friend_im_client::core::friends::StoredFriendRequest;
    let env = Env::start().await;
    let (alice, _) = env.account("boundary-a", "A").await;
    let (bob, key) = env.account("boundary-b", "B").await;
    let mut other = env.device("boundary-b", "B2").await;
    other
        .client
        .join_with_recovery(&key.to_words())
        .await
        .unwrap();
    other
        .wait("active", |u| matches!(u, Update::Activated).then_some(()))
        .await;
    let link = bob.client.friend_link(false).await.unwrap();
    let id = RequestId::random();
    alice
        .client
        .request_link_friend(&id, &link, "仅双方可见")
        .await
        .unwrap();
    let db = env.server.db();
    let json: String = db
        .query_row(
            "SELECT record FROM friend_requests WHERE request=?1",
            [id.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    let mut record: StoredFriendRequest = serde_json::from_str(&json).unwrap();
    record.expires_ms = friend_im_server::now_ms() - 1;
    db.execute(
        "UPDATE friend_requests SET expires_ms=?1,record=?2 WHERE request=?3",
        rusqlite::params![
            record.expires_ms,
            serde_json::to_string(&record).unwrap(),
            id.as_str()
        ],
    )
    .unwrap();
    assert_eq!(
        bob.client.accept_friend(&id).await.unwrap().record.status,
        Status::Expired
    );
    let second = RequestId::random();
    alice
        .client
        .resend_friend_request(&id, &second, "重新申请")
        .await
        .unwrap();
    bob.client
        .close_friend_request(&second, false)
        .await
        .unwrap();
    assert_eq!(
        alice
            .client
            .resend_friend_request(&second, &RequestId::random(), "")
            .await
            .unwrap_err()
            .code(),
        Some(ErrorCode::RateLimited)
    );
    let json: String = db
        .query_row(
            "SELECT record FROM friend_requests WHERE request=?1",
            [second.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    let mut record: StoredFriendRequest = serde_json::from_str(&json).unwrap();
    record.updated_ms -= 24 * 60 * 60 * 1000 + 1;
    db.execute(
        "UPDATE friend_requests SET updated_ms=?1,record=?2 WHERE request=?3",
        rusqlite::params![
            record.updated_ms,
            serde_json::to_string(&record).unwrap(),
            second.as_str()
        ],
    )
    .unwrap();
    let third = RequestId::random();
    alice
        .client
        .resend_friend_request(&second, &third, "")
        .await
        .unwrap();
    db.execute_batch("CREATE TRIGGER fail_approval BEFORE INSERT ON conv_keys BEGIN SELECT RAISE(ABORT,'test transaction rollback'); END;").unwrap();
    assert!(bob.client.accept_friend(&third).await.is_err());
    assert!(alice.client.convs().await.unwrap().is_empty());
    assert_eq!(
        bob.client
            .friend_requests(true, None, None)
            .await
            .unwrap()
            .requests[0]
            .record
            .status,
        Status::Pending
    );
    db.execute_batch("DROP TRIGGER fail_approval;").unwrap();
    let (first, second) = tokio::join!(
        bob.client.accept_friend(&third),
        other.client.accept_friend(&third)
    );
    assert_eq!(first.unwrap().record, second.unwrap().record);
    assert_eq!(alice.client.convs().await.unwrap().len(), 1);
}

#[tokio::test]
async fn reject_withdraw_and_link_revocation_do_not_create_friends() {
    let env = Env::start().await;
    let (alice, _) = env.account("close-a", "A").await;
    let (bob, _) = env.account("close-b", "B").await;
    let link = bob.client.friend_link(false).await.unwrap();
    let id = RequestId::random();
    alice
        .client
        .request_link_friend(&id, &link, "")
        .await
        .unwrap();
    assert!(bob.client.close_friend_request(&id, true).await.is_err());
    assert_eq!(
        bob.client
            .close_friend_request(&id, false)
            .await
            .unwrap()
            .record
            .status,
        Status::Rejected
    );
    assert_eq!(
        bob.client.accept_friend(&id).await.unwrap().record.status,
        Status::Rejected
    );
    assert_eq!(
        alice
            .client
            .request_link_friend(&RequestId::random(), &link, "")
            .await
            .unwrap_err()
            .code(),
        Some(ErrorCode::RateLimited)
    );
    let (carol, _) = env.account("close-c", "C").await;
    let c = RequestId::random();
    carol
        .client
        .request_link_friend(&c, &link, "")
        .await
        .unwrap();
    assert_eq!(
        carol
            .client
            .close_friend_request(&c, true)
            .await
            .unwrap()
            .record
            .status,
        Status::Withdrawn
    );
    let (dave, _) = env.account("close-d", "D").await;
    let d = RequestId::random();
    dave.client
        .request_link_friend(&d, &link, "")
        .await
        .unwrap();
    bob.client.friend_link(true).await.unwrap();
    assert_eq!(
        bob.client.accept_friend(&d).await.unwrap().record.status,
        Status::Expired
    );
    assert!(bob.client.convs().await.unwrap().is_empty());
}

#[tokio::test]
async fn cursor_pages_rate_limit_and_withdraw_accept_race() {
    use friend_im_client::core::{
        friends::StoredFriendRequest,
        wire::{Request, Response},
    };
    let env = Env::start().await;
    let (alice, _) = env.account("pages-a", "A").await;
    let (bob, _) = env.account("pages-b", "B").await;
    let link = bob.client.friend_link(false).await.unwrap();
    let db = env.server.db();
    let mut ids = Vec::new();
    for _ in 0..30 {
        let id = RequestId::random();
        let mut record = alice
            .client
            .request_link_friend(&id, &link, "page")
            .await
            .unwrap()
            .record;
        record.expires_ms = friend_im_server::now_ms() - 1;
        db.execute(
            "UPDATE friend_requests SET expires_ms=?1,record=?2 WHERE request=?3",
            rusqlite::params![
                record.expires_ms,
                serde_json::to_string(&record).unwrap(),
                id.as_str()
            ],
        )
        .unwrap();
        ids.push(id);
    }
    assert_eq!(
        alice
            .client
            .request_link_friend(&RequestId::random(), &link, "")
            .await
            .unwrap_err()
            .code(),
        Some(ErrorCode::RateLimited)
    );
    let (mut raw, _) =
        Raw::connect(&env.server.url(), &bob.client.credentials().await, "paging").await;
    let mut cursor = None;
    let mut found = Vec::new();
    loop {
        let Response::FriendRequests {
            requests,
            next,
            pending,
        } = raw
            .request(Request::FriendRequests {
                before: cursor,
                limit: 7,
                incoming: true,
                peer: Some(alice.client.uid().clone()),
            })
            .await
            .unwrap()
        else {
            panic!("wrong page")
        };
        assert!(requests.len() <= 7);
        assert_eq!(pending, 0);
        found.extend(requests.into_iter().map(|r| r.envelope.body.id));
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    ids.reverse();
    assert_eq!(found, ids);
    for limit in [0, 101] {
        assert_eq!(
            raw.request(Request::FriendRequests {
                before: None,
                limit,
                incoming: true,
                peer: None
            })
            .await
            .unwrap_err()
            .code,
            ErrorCode::BadRequest
        );
    }
    // Move only the test ledger's hourly window, preserving signed envelopes.
    db.execute(
        "UPDATE friend_requests SET created_ms=created_ms-3600001",
        [],
    )
    .unwrap();
    let race = RequestId::random();
    alice
        .client
        .request_link_friend(&race, &link, "")
        .await
        .unwrap();
    let (withdraw, accept) = tokio::join!(
        alice.client.close_friend_request(&race, true),
        bob.client.accept_friend(&race)
    );
    assert_eq!(
        withdraw.unwrap().record.status,
        accept.unwrap().record.status
    );
    let stored: String = db
        .query_row(
            "SELECT record FROM friend_requests WHERE request=?1",
            [race.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    let record: StoredFriendRequest = serde_json::from_str(&stored).unwrap();
    assert!(matches!(
        record.status,
        Status::Withdrawn | Status::Accepted
    ));
    assert_eq!(
        alice.client.convs().await.unwrap().len(),
        usize::from(record.status == Status::Accepted)
    );
}

#[tokio::test]
async fn old_friend_data_migrates_and_invalid_sources_cannot_be_approved() {
    use friend_im_client::core::conv::GroupProfile;
    let mut env = Env::start().await;
    let (mut alice, _) = env.account("migration-a", "A").await;
    let (mut bob, _) = env.account("migration-b", "B").await;
    let old = approved_dm(&alice.client, &bob.client).await;
    alice
        .client
        .send(&old, &MsgId::random(), text("old encrypted history"))
        .await
        .unwrap();
    bob.wait_text("old encrypted history").await;
    // Recreate the pre-approval schema while retaining real account, key and message data.
    env.server
        .db()
        .execute_batch(
            "DROP TABLE friend_requests; UPDATE meta SET value='2' WHERE key='schema_version';",
        )
        .unwrap();
    alice
        .wait("connected", |u| {
            matches!(u, Update::Connected { .. }).then_some(())
        })
        .await;
    env.server.restart().await;
    alice
        .wait("disconnected", |u| {
            matches!(u, Update::Disconnected).then_some(())
        })
        .await;
    alice
        .wait("reconnected", |u| {
            matches!(u, Update::Connected { .. }).then_some(())
        })
        .await;
    assert_eq!(approved_dm(&alice.client, &bob.client).await, old);
    alice
        .client
        .send(&old, &MsgId::random(), text("old friend after migration"))
        .await
        .unwrap();
    bob.wait_text("old friend after migration").await;
    assert!(
        env.server
            .db()
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE conv=?1",
                [old.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap()
            >= 2
    );
    let (carol, _) = env.account("migration-c", "C").await;
    approved_dm(&carol.client, &alice.client).await;
    let group = alice
        .client
        .create_group(
            &GroupProfile {
                name: "source".into(),
                ..Default::default()
            },
            &[bob.client.uid().clone(), carol.client.uid().clone()],
        )
        .await
        .unwrap();
    let id = RequestId::random();
    bob.client
        .request_group_friend(&id, carol.client.uid(), &group, "")
        .await
        .unwrap();
    alice.client.kick(&group, bob.client.uid()).await.unwrap();
    assert_eq!(
        carol.client.accept_friend(&id).await.unwrap().record.status,
        Status::Expired
    );
    let id = RequestId::random();
    bob.client
        .request_link_friend(&id, &carol.client.friend_link(false).await.unwrap(), "")
        .await
        .unwrap();
    bob.client.reset().await.unwrap();
    assert_eq!(
        carol.client.accept_friend(&id).await.unwrap().record.status,
        Status::Expired
    );
}

#[tokio::test]
async fn recipient_pending_and_total_ledger_capacity_are_bounded() {
    let env = Env::start().await;
    let (alice, _) = env.account("capacity-a", "A").await;
    let (bob, _) = env.account("capacity-b", "B").await;
    let (carol, _) = env.account("capacity-c", "C").await;
    let link = bob.client.friend_link(false).await.unwrap();
    let id = RequestId::random();
    let record = alice
        .client
        .request_link_friend(&id, &link, "")
        .await
        .unwrap()
        .record;
    let db = env.server.db();
    // Counter fixtures avoid thousands of cryptographic accounts: these rows are
    // never returned to a client; ordinary records and paging are tested above.
    db.execute("WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<99) INSERT INTO friend_requests(request,from_uid,to_uid,pair_key,status,created_ms,expires_ms,updated_ms,record) SELECT 'capacity-'||x,'fixture-'||x,?1,'pair-'||x,'pending',?2,?3,?2,?4 FROM n",rusqlite::params![bob.client.uid().as_str(),record.created_ms,record.expires_ms,serde_json::to_string(&record).unwrap()]).unwrap();
    assert_eq!(
        carol
            .client
            .request_link_friend(&RequestId::random(), &link, "")
            .await
            .unwrap_err()
            .code(),
        Some(ErrorCode::RateLimited)
    );
    db.execute("UPDATE friend_requests SET status='expired'", [])
        .unwrap();
    db.execute("WITH RECURSIVE n(x) AS (SELECT 100 UNION ALL SELECT x+1 FROM n WHERE x<9999) INSERT INTO friend_requests(request,from_uid,to_uid,pair_key,status,created_ms,expires_ms,updated_ms,record) SELECT 'capacity-'||x,'fixture-'||x,?1,'pair-'||x,'expired',?2,?3,?2,?4 FROM n",rusqlite::params![bob.client.uid().as_str(),record.created_ms,record.expires_ms,serde_json::to_string(&record).unwrap()]).unwrap();
    assert_eq!(
        carol
            .client
            .request_link_friend(&RequestId::random(), &link, "")
            .await
            .unwrap_err()
            .code(),
        Some(ErrorCode::RateLimited)
    );
}
