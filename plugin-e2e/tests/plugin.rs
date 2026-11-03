//! 插件端到端（方案 plan-v1 §13.4）：真的服务器 + 真的插件（进程内，和程序同一套代码），测试扮演 HHH 讲协议。
//! 对照契约 S14（收发消息）、S13（进出群的提示）、S16（对方重置）、S19（断网与补收）、S22（退出登录）在插件这一层的链路；
//! Friend 里的界面、登录与真实账号在第三阶段验。
//!
//! 每台“插件电脑”都是账号用恢复密钥新加的一台电脑：插件拿它的凭据上线，和真实情况一样是独立的一台。

#[path = "../../tests/common/mod.rs"]
mod common;
mod host;

use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use common::*;
use friend_im::credential::Credential;
use friend_im_client::core::conv::GroupProfile;
use friend_im_client::core::ids::{ConvId, Uid};
use friend_im_client::core::keys::RecoveryKey;
use friend_im_client::{Credentials, Update};
use hhh_im_protocol::types::{Block, Event, Message, SenderKind};
use host::{Host, events, resident_bytes, text_of};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// 账号用恢复密钥新加一台电脑，交给插件的凭据。
async fn plugin_computer(env: &Env, member: &str, recovery: &RecoveryKey, name: &str) -> String {
    let mut peer = env.device(member, name).await;
    peer.client.join_with_recovery(&recovery.to_words()).await.unwrap();
    peer.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    let credential = Credential::new(&env.server.url(), peer.client.uid().as_str(), name, peer.client.credentials().await);
    peer.client.close();
    credential.encode()
}

/// 起一个插件并上线。
async fn online(env: &Env, credential: &str) -> (Host, tempfile::TempDir) {
    let temp = tempfile::tempdir().unwrap();
    let (host, _) = Host::start(temp.path()).await;
    let opened = host.open(&env.server.url(), Some(credential), None).await;
    assert_eq!(opened["state"], "online", "{opened}");
    (host, temp)
}

fn conversation(conv: &ConvId) -> Value {
    json!({ "type": "conversation", "conversationId": conv.to_string() })
}

async fn send(host: &Host, op: &str, target: Value, content: Value) -> Message {
    let result = host.call("messages/send", json!({ "operationId": op, "target": target, "content": content })).await.unwrap();
    assert_eq!(result["state"], "accepted", "{result}");
    serde_json::from_value(result["message"].clone()).unwrap()
}

async fn send_text(host: &Host, conv: &ConvId, text: &str) -> Message {
    send(host, &format!("op-{text}"), conversation(conv), json!([{ "type": "text", "text": text }])).await
}

fn sha256_hex(data: &[u8]) -> String {
    Sha256::digest(data).iter().map(|b| format!("{b:02x}")).collect()
}

/// 按协议上传一份附件（每段 1 MiB），返回发送时用的键。
async fn upload(host: &Host, kind: &str, name: &str, data: &[u8]) -> String {
    let begin = host.call("uploads/begin", json!({ "kind": kind, "name": name, "bytes": data.len() })).await.unwrap();
    let id = begin["uploadId"].as_str().unwrap().to_owned();
    for (i, chunk) in data.chunks(1 << 20).enumerate() {
        host.call("uploads/chunk", json!({ "uploadId": id, "offset": i << 20, "data": BASE64.encode(chunk) })).await.unwrap();
    }
    let finish = host.call("uploads/finish", json!({ "uploadId": id, "sha256": sha256_hex(data) })).await.unwrap();
    finish["key"].as_str().unwrap().to_owned()
}

/// 按宿主的做法（一段 1 MiB，直到 eof）读一个资源。
async fn read_all(host: &Host, message: &Message, block: usize) -> Vec<u8> {
    let Block::Resource { resource, .. } = &message.content[block] else { panic!("第 {block} 块不是资源") };
    let mut out = Vec::new();
    loop {
        let chunk = host
            .call(
                "resources/read",
                json!({ "conversationId": message.conversation_id, "messageId": message.id, "key": resource.key,
                        "kind": resource.kind, "offset": out.len(), "length": 1 << 20 }),
            )
            .await
            .unwrap();
        assert_eq!(chunk["total"], resource.bytes.unwrap());
        out.extend(BASE64.decode(chunk["data"].as_str().unwrap()).unwrap());
        if chunk["eof"].as_bool().unwrap() {
            return out;
        }
    }
}

/// 一张“PNG”：文件头写着宽 3、高 2，后面是填充，凑到 `size` 字节（插件只读文件头取宽高）。
fn png(size: usize) -> Vec<u8> {
    let mut data = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 13, b'I', b'H', b'D', b'R'];
    data.extend(3u32.to_be_bytes());
    data.extend(2u32.to_be_bytes());
    data.extend([8, 6, 0, 0, 0, 0, 0, 0, 0]);
    data.extend((0..size - data.len()).map(|i| (i % 251) as u8));
    data
}

/// 发出去之后宿主把附件存进文件库（真宿主的 `keep_sent_attachment`）。
fn keep_sent(host: &Host, message: &Message, block: usize, data: &[u8]) {
    let Block::Resource { resource, .. } = &message.content[block] else { panic!("第 {block} 块不是资源") };
    host.library.files.lock().unwrap().insert(resource.key.clone(), data.to_vec());
}

/// 等两台电脑的宿主库在这个会话里逐条一样（编号、时间、是否撤回、顺序）。
async fn wait_identical(hosts: &[&Host], conv: &ConvId) {
    let row = |m: &Message| (m.id.clone(), m.created_at_ms, m.deleted);
    let deadline = Instant::now() + host::WAIT;
    loop {
        let reference: Vec<_> = hosts[0].stored(conv.as_str()).iter().filter(|m| m.raw.is_some()).map(row).collect();
        if hosts.iter().all(|h| h.stored(conv.as_str()).iter().filter(|m| m.raw.is_some()).map(row).collect::<Vec<_>>() == reference) {
            return;
        }
        assert!(Instant::now() < deadline, "几台电脑对不上");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

// ---------- S14：收发消息、同步、撤回、附件、翻历史、会话列表 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reading_protocol_keeps_exact_positions_partial_lists_and_notification_negotiation() {
    let env=Env::start().await;
    let (alice,alice_key)=env.account("alice","phone").await;
    let (bob,bob_key)=env.account("bob","phone").await;
    let (mut carol,_)=env.account("carol","phone").await;
    approved_dm(&alice.client, &bob.client).await;
    approved_dm(&alice.client, &carol.client).await;
    let group=alice.client.create_group(&GroupProfile::default(),&[bob.client.uid().clone(),carol.client.uid().clone()]).await.unwrap();
    let alice_credential=plugin_computer(&env,"alice",&alice_key,"plugin").await;
    let bob_credential=plugin_computer(&env,"bob",&bob_key,"plugin").await;
    let temp=tempfile::tempdir().unwrap();
    let (a,init)=Host::start_reading(temp.path()).await;
    assert_eq!(init["capabilities"]["receipts"]["readers"],true);
    assert_eq!(init["capabilities"]["receipts"]["unreaders"],true);
    assert_eq!(a.open(&env.server.url(),Some(&alice_credential),None).await["state"],"online");
    let (b,_temp)=online(&env,&bob_credential).await;
    let first=send_text(&a,&group,"阅读边界一").await;
    let second=send_text(&a,&group,"阅读边界二").await;
    b.wait_text("阅读边界二").await;
    let scope=json!({"conversationId":group.to_string()});
    let before=a.call("reading/receipts",json!({"scope":scope,"messageIds":[second.id]})).await.unwrap();
    assert!(before["items"][0].get("readCount").is_none(),"未报告阅读的人不能当成零人已读");
    assert!(before["items"][0].get("totalCount").is_none(),"没有事实的人不当未读");
    let sync=b.call("reading/sync",json!({"scope":scope,"messageIds":[]})).await.unwrap();
    assert!(sync.get("through").is_none());
    assert_eq!(b.call("reading/mark",json!({"scope":scope,"messageId":first.id})).await.unwrap()["state"],"applied");
    let through=b.call("reading/sync",json!({"scope":scope,"messageIds":[]})).await.unwrap()["through"].clone();
    assert_eq!(through.as_str(),first.read_position.as_deref(),"读到的位置就是那条消息的位置");
    let carol_message=carol.wait_text("阅读边界二").await;
    carol.mark_read(&group,&carol_message).await.unwrap();
    let partial=host::eventually("carol 的已读到 alice",async||{
        let found=a.call("reading/receipts",json!({"scope":scope,"messageIds":[second.id]})).await.unwrap();
        (found["items"][0]["totalCount"]==2).then_some(found)
    }).await;
    assert_eq!(partial["items"][0]["readCount"],1);
    let unread=a.call("reading/people",json!({"scope":scope,"messageId":second.id,"read":false,"limit":1})).await.unwrap();
    assert_eq!(unread["people"][0]["id"],bob.client.uid().as_str());
    assert_eq!(unread["complete"],true);
    b.call("reading/mark",json!({"scope":scope,"messageId":second.id})).await.unwrap();
    let page=host::eventually("bob 的已读到 alice",async||{
        let found=a.call("reading/people",json!({"scope":scope,"messageId":second.id,"read":true,"limit":1})).await.unwrap();
        (found["complete"]==false).then_some(found)
    }).await;
    let next=page["nextCursor"].as_str().unwrap();
    let end=a.call("reading/people",json!({"scope":scope,"messageId":second.id,"read":true,"limit":1,"cursor":next})).await.unwrap();
    assert_eq!(end["complete"],true);
    assert_ne!(page["people"][0]["id"],end["people"][0]["id"]);
    assert!(a.call("reading/people",json!({"scope":scope,"messageId":first.id,"read":true,"limit":1,"cursor":next})).await.is_err());
    a.wait("已协商的阅读通知",|seen|seen.iter().any(|s|matches!(s,host::Seen::Note(method,_) if method=="reading/changed")).then_some(())).await;
    assert!(!b.seen.lock().unwrap().iter().any(|s|matches!(s,host::Seen::Note(method,_) if method=="reading/changed")),"未协商的宿主不会收到新通知");
    let invalid=b.call("reading/mark",json!({"scope":scope,"messageId":second.id,"position":"x"})).await;
    assert!(invalid.is_err());
    a.close().await;
    b.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn messages_flow_through_plugins() {
    let env = Env::start().await;
    let (mut alice_phone, alice_key) = env.account("alice", "alice 的手机").await;
    let (bob_pc, bob_key) = env.account("bob", "bob 的台式机").await;
    let (carol, _) = env.account("carol", "carol 的电脑").await;
    let dm = approved_dm(&bob_pc.client, &alice_phone.client).await;
    approved_dm(&carol.client, &alice_phone.client).await;
    let group = alice_phone
        .client
        .create_group(&GroupProfile { name: "周末爬山".into(), ..Default::default() }, &[bob_pc.client.uid().clone(), carol.client.uid().clone()])
        .await
        .unwrap();
    let (alice_uid, bob_uid, carol_uid) = (alice_phone.client.uid().clone(), bob_pc.client.uid().clone(), carol.client.uid().clone());

    let a_cred = plugin_computer(&env, "alice", &alice_key, "alice 的插件电脑").await;
    let b_cred = plugin_computer(&env, "bob", &bob_key, "bob 的插件电脑").await;
    let (a, _a_dir) = online(&env, &a_cred).await;
    let (b, _b_dir) = online(&env, &b_cred).await;

    // 会话列表、资料、成员、名字。
    let list = a.call("conversations/list", json!({ "limit": 100 })).await.unwrap();
    let convs: Vec<Value> = list["items"].as_array().unwrap().clone();
    let dm_item = convs.iter().find(|c| c["id"] == dm.to_string()).expect("私聊在列表里");
    assert_eq!((dm_item["kind"].as_str(), dm_item["name"].as_str(), dm_item["peer"].as_str()), (Some("direct"), Some("bob"), Some(bob_uid.as_str())));
    let group_item = convs.iter().find(|c| c["id"] == group.to_string()).expect("群在列表里");
    assert_eq!((group_item["kind"].as_str(), group_item["name"].as_str()), (Some("group"), Some("周末爬山")));
    let paged = a.call("conversations/list", json!({ "limit": 1 })).await.unwrap();
    assert_eq!(paged["items"].as_array().unwrap().len(), 1);
    assert!(paged["next"].is_string(), "一页一个时还有下一页");
    let members = a.call("conversations/members", json!({ "conversationId": group.to_string(), "limit": 50 })).await.unwrap();
    let mut names: Vec<&str> = members["items"].as_array().unwrap().iter().map(|p| p["name"].as_str().unwrap()).collect();
    names.sort();
    assert_eq!(names, ["alice", "bob", "carol"]);
    let people = a.call("people/get", json!({ "ids": [carol_uid.to_string(), "不是编号", Uid::from_raw([7; 16]).to_string()] })).await.unwrap();
    assert_eq!(people["found"][0]["name"], "carol");
    assert_eq!(people["missing"].as_array().unwrap().len(), 2, "{people}");
    assert!(a.call("conversations/get", json!({ "conversationId": "不是编号" })).await.is_err());

    // 文字：点发送到对方收到（契约 §7：2 秒内）；同一账号的另一台电脑（手机）也收到。
    let started = Instant::now();
    let hello = send_text(&a, &dm, "你好，这是插件发的").await;
    let got = b.wait_text("你好，这是插件发的").await;
    assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
    assert_eq!((got.id.as_str(), got.sender.id.as_str(), got.sender.name.as_deref()), (hello.id.as_str(), alice_uid.as_str(), Some("alice")));
    assert_eq!(got.created_at_ms, hello.created_at_ms, "两边同一个时间（发送方签名的时间）");
    alice_phone.wait_text("你好，这是插件发的").await;

    // @人、@所有人、回复。
    send(
        &b,
        "op-mention",
        conversation(&group),
        json!([{ "type": "mention", "userId": alice_uid.to_string(), "name": "alice" }, { "type": "text", "text": " 周六几点" }, { "type": "mentionAll" }]),
    )
    .await;
    let mention = a.wait_text(" 周六几点").await;
    assert!(matches!(&mention.content[0], Block::Mention { user_id: Some(u), name: Some(n), .. } if *u == alice_uid.to_string() && n == "alice"));
    assert!(matches!(&mention.content[2], Block::Mention { everyone: true, .. }));
    let reply = send(
        &a,
        "op-reply",
        json!({ "type": "reply", "conversationId": group.to_string(), "messageId": mention.id }),
        json!([{ "type": "text", "text": "九点" }]),
    )
    .await;
    assert_eq!(reply.reply_to.as_deref(), Some(mention.id.as_str()));
    assert_eq!(b.wait_text("九点").await.reply_to.as_deref(), Some(mention.id.as_str()));

    // 图片（200 KiB，跨好几段加密块）与文件（1.5 MiB，按 1 MiB 一段读两次）。
    let image = png(200 * 1024);
    let key = upload(&a, "image", "山顶.png", &image).await;
    let sent = send(&a, "op-image", conversation(&dm), json!([{ "type": "image", "key": key }])).await;
    keep_sent(&a, &sent, 0, &image);
    let got = b.wait_message("推来图片", |m| m.id == sent.id).await;
    let Block::Resource { resource, name, width, height, .. } = &got.content[0] else { panic!("不是资源") };
    assert_eq!((resource.kind, resource.mime.as_deref(), resource.bytes), (hhh_im_protocol::types::ResourceKind::Image, Some("image/png"), Some(image.len() as u64)));
    assert_eq!((name.as_deref(), *width, *height), (Some("山顶.png"), Some(3), Some(2)));
    assert!(read_all(&b, &got, 0).await == image, "读出来和发的一样");
    let doc: Vec<u8> = (0..(1536 * 1024 + 9)).map(|i: u32| (i.wrapping_mul(13) % 241) as u8).collect();
    let key = upload(&b, "file", "路线.gpx", &doc).await;
    let sent = send(&b, "op-file", conversation(&dm), json!([{ "type": "text", "text": "路线" }, { "type": "file", "key": key }])).await;
    keep_sent(&b, &sent, 1, &doc);
    let got = a.wait_message("推来文件", |m| m.id == sent.id).await;
    assert!(read_all(&a, &got, 1).await == doc);
    // 用过的暂存键发出去就删了：再发要重新上传。
    let again = a.call("messages/send", json!({ "operationId": "op-reuse", "target": conversation(&dm), "content": [{ "type": "file", "key": key }] })).await;
    assert!(again.is_err(), "暂存键不能用第二次");

    // 上传的边界：摘要不对、位置不对、超过 100 MiB。
    let begin = a.call("uploads/begin", json!({ "kind": "file", "name": "x.bin", "bytes": 3 })).await.unwrap();
    let id = begin["uploadId"].as_str().unwrap();
    assert!(a.call("uploads/chunk", json!({ "uploadId": id, "offset": 1, "data": BASE64.encode([1]) })).await.is_err());
    a.call("uploads/chunk", json!({ "uploadId": id, "offset": 0, "data": BASE64.encode([1, 2, 3]) })).await.unwrap();
    assert!(a.call("uploads/finish", json!({ "uploadId": id, "sha256": "00" })).await.is_err());
    let too_big = a.call("uploads/begin", json!({ "kind": "file", "name": "big.bin", "bytes": 101u64 << 20 })).await.unwrap_err();
    assert_eq!(too_big.data.unwrap().kind, hhh_im_protocol::ErrorKind::LimitExceeded);

    // 同一个操作编号再发：同一条，所有设备上只有一条；lookupSend 给同一个结果。
    let first = send_text(&a, &dm, "只该有一条").await;
    let second = send_text(&a, &dm, "只该有一条").await;
    assert_eq!(first.id, second.id);
    let looked = a.call("messages/lookupSend", json!({ "operationId": "op-只该有一条" })).await.unwrap();
    assert_eq!(looked["message"]["id"], first.id);

    // 撤回：对方那边同一条变成“已撤回”，时间不变。
    let doomed = send_text(&a, &dm, "这条要撤回").await;
    b.wait_text("这条要撤回").await;
    a.call("messages/recall", json!({ "conversationId": dm.to_string(), "messageId": doomed.id })).await.unwrap();
    let gone = b.wait_message("推来已撤回", |m| m.id == doomed.id && m.deleted).await;
    assert!(gone.content.is_empty());
    assert_eq!(gone.created_at_ms, doomed.created_at_ms);
    // 不能撤回别人的。
    let theirs = b.wait_text("路线").await;
    assert!(a.call("messages/recall", json!({ "conversationId": dm.to_string(), "messageId": theirs.id })).await.is_err());

    // 宿主里系统提示在，撤回的那条是已撤回，只有一条“只该有一条”。平台不再读历史：两边的宿主库逐条一样。
    wait_identical(&[&a, &b], &dm).await;
    let stored = b.stored(dm.as_str());
    assert!(stored.iter().any(|m| text_of(m) == "通过好友链接添加，身份已核对" && m.sender.kind == SenderKind::System));
    assert_eq!(stored.iter().filter(|m| text_of(m) == "只该有一条").count(), 1);
    assert!(stored.iter().any(|m| m.id == doomed.id && m.deleted));
    assert!(b.call("messages/list", json!({ "conversationId": dm.to_string(), "newestFirst": true, "limit": 10 })).await.is_err(), "平台不再提供读历史");

    // 推送带着同步位置；插件的输出全是协议消息。
    assert!(b.last_sync_state().is_some());
    assert!(a.bad_lines.lock().unwrap().is_empty() && b.bad_lines.lock().unwrap().is_empty());
}

// ---------- 推送：宿主没收下就算没写好，下次对账再来；重启后索引从宿主重建，只补缺的 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pushes_are_retried_and_resume_from_the_sync_state() {
    let env = Env::start().await;
    let (alice, _) = env.account("alice", "alice 的电脑").await;
    let (bob_pc, bob_key) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob_pc.client, &alice.client).await;
    let b_cred = plugin_computer(&env, "bob", &bob_key, "bob 的插件电脑").await;
    let (b, _dir) = online(&env, &b_cred).await;
    alice.send_as(&dm, &friend_im_client::core::ids::MsgId::random(), text("先到的一条")).await.unwrap();
    b.wait_text("先到的一条").await;

    // 宿主先不收：这条算没写好，宿主里没有。
    b.accept_pushes.store(false, std::sync::atomic::Ordering::SeqCst);
    alice.send_as(&dm, &friend_im_client::core::ids::MsgId::random(), text("宿主没收下的")).await.unwrap();
    b.wait("试过推送", |seen| events(seen).iter().any(|e| matches!(e, Event::Messages { messages, .. } if messages.iter().any(|m| text_of(m) == "宿主没收下的"))).then_some(())).await;
    assert!(!b.stored(dm.as_str()).iter().any(|m| text_of(m) == "宿主没收下的"));

    // 插件关掉期间来了三条；重启后对账补齐，之前收过的不再推。
    // 旧宿主到插件退出都不收：插件退出前还会重推，收了这条就在重启前进了库。
    let state = b.last_sync_state();
    let library = b.library.clone();
    b.close().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!b.stored(dm.as_str()).iter().any(|m| text_of(m) == "宿主没收下的"), "重启前宿主库里还没有");
    for i in 0..3 {
        alice.send_as(&dm, &friend_im_client::core::ids::MsgId::random(), text(&format!("关着时的第{i}条"))).await.unwrap();
    }
    let temp = tempfile::tempdir().unwrap();
    let (b2, _) = Host::restart(temp.path(), library).await;
    assert_eq!(b2.open(&env.server.url(), Some(&b_cred), state.as_deref()).await["state"], "online");
    for i in 0..3 {
        b2.wait_text(&format!("关着时的第{i}条")).await;
    }
    b2.wait_text("宿主没收下的").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!b2.pushed_messages().iter().any(|m| text_of(m) == "先到的一条"), "宿主已有的不再推");
}

// ---------- S19：断网：状态是“连接中”，本机保存就算发出；恢复后互相补齐 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnects_show_connecting_and_catch_up_afterwards() {
    let env = Env::start().await;
    let (alice, alice_key) = env.account("alice", "alice 的电脑").await;
    // bob 直连服务器，alice 的插件经过可以断网的代理。
    let direct = format!("http://{}", env.server.direct);
    let mut bob = Peer::connect(&direct, "bob 的电脑", Credentials::generate(), Some(env.cindy.token("bob", "bob")), None).await.unwrap();
    bob.client.provision().await.unwrap();
    bob.wait("开通后可用", |u| matches!(u, Update::Activated).then_some(())).await;
    let dm = approved_dm(&bob.client, &alice.client).await;
    let a_cred = plugin_computer(&env, "alice", &alice_key, "alice 的插件电脑").await;
    let (a, _dir) = online(&env, &a_cred).await;
    send_text(&a, &dm, "断网前").await;
    bob.wait_text("断网前").await;

    env.server.proxy.cut();
    a.wait_note("status", |p| p["state"] == "connecting").await;
    // 断着网发：本机保存了就算发出，等对账送达（契约 friend-im v10 第 3 项）。
    let offline = json!({ "operationId": "op-offline", "target": conversation(&dm), "content": [{ "type": "text", "text": "断网时发的" }] });
    let sent = a.call("messages/send", offline.clone()).await.unwrap();
    assert_eq!(sent["state"], "accepted", "{sent}");
    // 断网期间对方发来的，恢复后补收。
    bob.send_as(&dm, &friend_im_client::core::ids::MsgId::random(), text("你断网时我发的")).await.unwrap();
    env.server.proxy.restore();
    a.wait_note("status", |p| p["state"] == "online").await;
    a.wait_text("你断网时我发的").await;
    bob.wait_text("断网时发的").await;
    // 宿主重试沿用同一操作编号：还是这一条，对方只有一条。
    let retried = a.call("messages/send", offline).await.unwrap();
    assert_eq!(retried["state"], "accepted", "{retried}");
    send_text(&a, &dm, "恢复后").await;
    bob.wait_text("恢复后").await;
    assert_eq!(bob.texts(&dm).await.iter().filter(|t| *t == "断网时发的").count(), 1);
}

// ---------- S22 与失效：退出登录、被移出、没有或不对的凭据、登录 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logout_removal_and_bad_credentials_need_login() {
    let env = Env::start().await;
    let (mut alice, alice_key) = env.account("alice", "alice 的电脑").await;
    let temp = tempfile::tempdir().unwrap();
    let (host, init) = Host::start(temp.path()).await;
    assert_eq!(init["provider"]["id"], "friend-im");
    let url = env.server.url();

    // 没有凭据、读不懂、换了服务器地址：都要重新登录。
    assert_eq!(host.open(&url, None, None).await["reason"], "noCredential");
    assert_eq!(host.open(&url, Some("乱码"), None).await["reason"], "credentialInvalid");
    let cred = plugin_computer(&env, "alice", &alice_key, "插件电脑一").await;
    assert_eq!(host.open("https://im.example.com", Some(&cred), None).await["reason"], "credentialInvalid");
    assert!(host.call("session/open", json!({ "credential": cred, "settings": { "serverUrl": "ftp://x" } })).await.is_err());
    // 这一版还不能登录：开一个流程，当场以不可重试的失败结束。
    let flow = host.call("login/start", json!({})).await.unwrap();
    let failed = host.wait_note("login/failed", |p| p["flowId"] == flow["flowId"]).await;
    assert_eq!(failed["retryable"], false);

    // 退出登录：这台电脑从“我的电脑”里移除，之后这份凭据上不了线。
    assert_eq!(host.open(&url, Some(&cred), None).await["state"], "online");
    let before = alice.client.devices().await.unwrap();
    assert!(before.iter().any(|d| d.name == "插件电脑一"));
    host.call("session/logout", json!({})).await.unwrap();
    let after = alice.client.devices().await.unwrap();
    assert!(!after.iter().any(|d| d.name == "插件电脑一"), "退出后不在我的电脑里");
    assert!(host.call("conversations/list", json!({ "limit": 10 })).await.is_err(), "下线了");
    assert_eq!(host.open(&url, Some(&cred), None).await["state"], "needsLogin");

    // 被别的电脑移出：插件报授权失效（revoked）并下线。
    let cred2 = plugin_computer(&env, "alice", &alice_key, "插件电脑二").await;
    assert_eq!(host.open(&url, Some(&cred2), None).await["state"], "online");
    let device = alice.client.devices().await.unwrap().into_iter().find(|d| d.name == "插件电脑二").unwrap();
    alice.client.remove_device(&device.id).await.unwrap();
    let expired = host.wait_note("session/expired", |_| true).await;
    assert_eq!(expired["reason"], "revoked");
    alice.settle(Duration::from_millis(200)).await;
    assert!(host.call("conversations/list", json!({ "limit": 10 })).await.is_err());
    let reopened = host.open(&url, Some(&cred2), None).await;
    assert_eq!((reopened["state"].as_str(), reopened["reason"].as_str()), (Some("needsLogin"), Some("revoked")));
}

// ---------- S11、S13、S16：群变化与对方重置都是系统消息；自己退群推“会话没有了” ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_changes_and_resets_become_system_messages() {
    let env = Env::start().await;
    let (alice, alice_key) = env.account("alice", "alice 的电脑").await;
    let (mut bob, bob_key) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &alice.client).await;
    let a_cred = plugin_computer(&env, "alice", &alice_key, "alice 的插件电脑").await;
    let b_cred = plugin_computer(&env, "bob", &bob_key, "bob 的插件电脑").await;
    let (a, _a_dir) = online(&env, &a_cred).await;
    let (b, _b_dir) = online(&env, &b_cred).await;

    // 建群：成员先收到会话本身，再收到“X 创建了群，加入了 Y”。
    let group = alice.client.create_group(&GroupProfile { name: "读书会".into(), ..Default::default() }, &[bob.client.uid().clone()]).await.unwrap();
    let created = b.wait_text("alice 创建了群，加入了 bob").await;
    assert_eq!(created.sender.kind, SenderKind::System);
    let order = events(&b.seen.lock().unwrap());
    let conv_at = order.iter().position(|e| matches!(e, Event::Conversation { conversation } if conversation.id == group.to_string())).expect("推了会话");
    let msg_at = order.iter().position(|e| matches!(e, Event::Messages { messages, .. } if messages.iter().any(|m| m.id == created.id))).unwrap();
    assert!(conv_at < msg_at, "先推会话再推它的消息");

    // 改群名：会话资料随之更新。
    alice.client.set_group_profile(&group, &GroupProfile { name: "周三读书会".into(), ..Default::default() }).await.unwrap();
    b.wait_text("alice 修改了群资料").await;
    b.wait("会话改名", |seen| {
        events(seen).iter().any(|e| matches!(e, Event::Conversation { conversation } if conversation.name == "周三读书会")).then_some(())
    })
    .await;

    // bob 退群：对方看到“bob 退出了群”，bob 自己的插件推“会话没有了”。
    bob.client.leave(&group).await.unwrap();
    a.wait_text("bob 退出了群").await;
    b.wait("会话没有了", |seen| events(seen).iter().any(|e| matches!(e, Event::ConversationRemoved { conversation_id } if *conversation_id == group.to_string())).then_some(())).await;
    bob.settle(Duration::from_millis(100)).await;

    // 对方重置账号：私聊里出现契约 S16 的提示；重置后的新电脑解不开以前的消息，它们不进宿主。
    alice.send_as(&dm, &friend_im_client::core::ids::MsgId::random(), text("重置前的私聊")).await.unwrap();
    b.wait_text("重置前的私聊").await;
    let mut fresh = env.device("bob", "bob 的新电脑").await;
    fresh.client.reset().await.unwrap();
    fresh.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    alice.send_as(&dm, &friend_im_client::core::ids::MsgId::random(), text("你重置之后")).await.unwrap();
    a.wait_text("bob 重置了账号，之后的消息用新钥匙加密。想确认是本人，请当面或通过别的渠道问一下").await;
    let fresh_cred = Credential::new(&env.server.url(), fresh.client.uid().as_str(), "bob 的新电脑", fresh.client.credentials().await).encode();
    fresh.client.close();
    let (c, _c_dir) = online(&env, &fresh_cred).await;
    c.wait_text("你重置之后").await;
    assert!(!c.stored(dm.as_str()).iter().any(|m| text_of(m) == "重置前的私聊"), "重置前的那条解不开");
}

// 重置过账号的人先开口（契约 S7“账号可以继续用”、S16）：在原来的私聊里直接发，好友收到这条和“X 重置了账号”的提示。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reset_account_can_write_to_its_friends_first() {
    let env = Env::start().await;
    let (alice, alice_key) = env.account("alice", "alice 的电脑").await;
    let (bob, _bob_key) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &alice.client).await;
    alice.send_as(&dm, &friend_im_client::core::ids::MsgId::random(), text("重置前")).await.unwrap();
    let a_cred = plugin_computer(&env, "alice", &alice_key, "alice 的插件电脑").await;
    let (a, _a_dir) = online(&env, &a_cred).await;

    let mut fresh = env.device("bob", "bob 的新电脑").await;
    fresh.client.reset().await.unwrap();
    fresh.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    let fresh_cred = Credential::new(&env.server.url(), fresh.client.uid().as_str(), "bob 的新电脑", fresh.client.credentials().await).encode();
    fresh.client.close();
    let (b, _b_dir) = online(&env, &fresh_cred).await;
    let sent = b.call("messages/send", json!({ "operationId": "op-after-reset", "target": conversation(&dm), "content": [{ "type": "text", "text": "我重置了，先说一句" }] })).await.unwrap();
    assert_eq!(sent["state"], "accepted", "{sent}");
    a.wait_text("我重置了，先说一句").await;
    a.wait_text("bob 重置了账号，之后的消息用新钥匙加密。想确认是本人，请当面或通过别的渠道问一下").await;
}

// 被移出后又被邀请回来（契约 S12：新成员能看到群里以前的全部消息）：宿主本机已有在群期间的消息、只缺不在群的那段，
// 插件把这段补推过来，不用宿主去翻历史。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_invited_back_gets_what_was_said_while_away() {
    let env = Env::start().await;
    let (alice, _alice_key) = env.account("alice", "alice 的电脑").await;
    let (bob, bob_key) = env.account("bob", "bob 的电脑").await;
    approved_dm(&bob.client, &alice.client).await;
    let b_cred = plugin_computer(&env, "bob", &bob_key, "bob 的插件电脑").await;
    let (b, _b_dir) = online(&env, &b_cred).await;

    let group = alice.client.create_group(&GroupProfile { name: "读书会".into(), ..Default::default() }, &[bob.client.uid().clone()]).await.unwrap();
    b.wait_text("alice 创建了群，加入了 bob").await;
    alice.send_as(&group, &friend_im_client::core::ids::MsgId::random(), text("在群的时候")).await.unwrap();
    b.wait_text("在群的时候").await;
    alice.client.kick(&group, bob.client.uid()).await.unwrap();
    b.wait_text("alice 把 bob 移出了群").await;
    alice.send_as(&group, &friend_im_client::core::ids::MsgId::random(), text("bob 不在的时候")).await.unwrap();
    alice.client.invite(&group, &[bob.client.uid().clone()]).await.unwrap();
    b.wait_text("alice 邀请 bob 加入了群").await;
    let missed = b.wait_text("bob 不在的时候").await;
    assert_eq!(missed.sender.id, alice.client.uid().to_string(), "补推的照原来的发送人");
    let pushed = events(&b.seen.lock().unwrap())
        .iter()
        .filter(|e| matches!(e, Event::Messages { messages, .. } if messages.iter().any(|m| text_of(m) == "在群的时候")))
        .count();
    assert_eq!(pushed, 1, "在群期间收过的不再推");
}

// ---------- 真的插件程序：标准输出只有协议消息，日志里没有正文，shutdown 后退出 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_real_program_speaks_only_protocol_and_keeps_text_out_of_logs() {
    // 正文、文件名、群名都带上这个暗号，最后查插件交给宿主的日志与通知里一处都没有。
    const SECRET: &str = "月光宝盒7f3a";
    let env = Env::start().await;
    let (mut alice, _) = env.account("alice", "alice 的电脑").await;
    let (bob, bob_key) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &alice.client).await;
    let b_cred = plugin_computer(&env, "bob", &bob_key, "bob 的插件电脑").await;
    let temp = tempfile::tempdir().unwrap();
    let (mut b, init) = Host::spawn(&host::program(), temp.path()).await;
    assert_eq!(init["provider"]["id"], "friend-im");
    assert_eq!(init["platform"]["history"], "none");
    assert_eq!(init["capabilities"]["storeRead"], true);
    assert_eq!(b.open(&env.server.url(), Some(&b_cred), None).await["state"], "online");

    // 收、发、回复、附件、建群：每样都过一遍插件。
    alice.send_as(&dm, &friend_im_client::core::ids::MsgId::random(), text(&format!("收到的{SECRET}"))).await.unwrap();
    let got = b.wait_text(&format!("收到的{SECRET}")).await;
    send(&b, "op-1", json!({ "type": "reply", "conversationId": dm.to_string(), "messageId": got.id }), json!([{ "type": "text", "text": format!("发出的{SECRET}") }])).await;
    alice.wait_text(&format!("发出的{SECRET}")).await;
    let doc = format!("文件里的{SECRET}").repeat(1000).into_bytes();
    let key = upload(&b, "file", &format!("{SECRET}.txt"), &doc).await;
    send(&b, "op-2", conversation(&dm), json!([{ "type": "file", "key": key }])).await;
    alice.client.create_group(&GroupProfile { name: format!("群{SECRET}"), ..Default::default() }, &[bob.client.uid().clone()]).await.unwrap();
    b.wait("推来群", |seen| events(seen).iter().any(|e| matches!(e, Event::Conversation { conversation } if conversation.name.contains(SECRET))).then_some(())).await;
    assert!(b.stored(dm.as_str()).iter().any(|m| text_of(m).contains(SECRET)));
    // 会被拒的请求（会写日志）。
    assert!(b.call("messages/send", json!({ "operationId": "op-3", "target": conversation(&dm), "content": [{ "type": "file", "key": SECRET }] })).await.is_err());

    // 退出：shutdown，宿主关标准输入，程序自己退出。
    b.call("shutdown", json!({})).await.unwrap();
    b.close().await;
    let mut child = b.child.take().unwrap();
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait()).await.expect("关了标准输入后 5 秒内退出（协议 §2）").unwrap();
    assert!(status.success(), "{status}");

    let bad = b.bad_lines.lock().unwrap().clone();
    assert!(bad.is_empty(), "标准输出里有不是协议消息的行：{bad:?}");
    let notes = b.notes_text();
    assert!(notes.contains("log/write"), "日志交给了宿主");
    assert!(!notes.contains(SECRET), "日志或通知里出现了正文：{notes}");
}

// ---------- friend-im v10 第 4 项：新设备从开着的旧设备拿到全部历史和文件 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_computer_gets_history_and_files_from_an_old_one() {
    let env = Env::start().await;
    let (alice, alice_key) = env.account("alice", "alice 的电脑").await;
    let (bob, _) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &alice.client).await;
    let a_cred = plugin_computer(&env, "alice", &alice_key, "旧电脑").await;
    let (a, _a_dir) = online(&env, &a_cred).await;
    send_text(&a, &dm, "旧电脑上的历史").await;
    let doomed = send_text(&a, &dm, "撤回的不补").await;
    a.call("messages/recall", json!({ "conversationId": dm.to_string(), "messageId": doomed.id })).await.unwrap();
    let doc: Vec<u8> = (0..(700 * 1024 + 9)).map(|i: u32| (i.wrapping_mul(17) % 241) as u8).collect();
    let key = upload(&a, "file", "相册.zip", &doc).await;
    let file = send(&a, "op-doc", conversation(&dm), json!([{ "type": "file", "key": key }])).await;
    keep_sent(&a, &file, 0, &doc);

    let c_cred = plugin_computer(&env, "alice", &alice_key, "新电脑").await;
    let (c, _c_dir) = online(&env, &c_cred).await;
    c.wait_text("旧电脑上的历史").await;
    wait_identical(&[&a, &c], &dm).await;
    assert!(c.stored(dm.as_str()).iter().any(|m| m.id == doomed.id && m.deleted), "撤回的是墓碑");
    assert!(c.stored(dm.as_str()).iter().any(|m| text_of(m) == "通过好友链接添加，身份已核对"), "系统提示也有");
    let copy = c.stored(dm.as_str()).into_iter().find(|m| m.id == file.id).unwrap();
    assert!(read_all(&c, &copy, 0).await == doc, "文件从旧电脑读");

    // 摘要随消息签名：资源第一段就带上，宿主收完自己核对。
    let Block::Resource { resource, .. } = &copy.content[0] else { panic!("不是资源") };
    let chunk = c
        .call("resources/read", json!({ "conversationId": dm.to_string(), "messageId": copy.id, "key": resource.key, "kind": resource.kind, "offset": 0, "length": 10 }))
        .await
        .unwrap();
    assert_eq!(chunk["sha256"].as_str().map(str::to_owned), Some(sha256_hex(&doc)));

    // 谁都没有这个文件：报暂时读不到（可以重试），不伪造成功。
    a.library.files.lock().unwrap().clear();
    let refused = c
        .call("resources/read", json!({ "conversationId": dm.to_string(), "messageId": copy.id, "key": resource.key, "kind": resource.kind, "offset": 0, "length": 10 }))
        .await
        .unwrap_err();
    assert_eq!(refused.data.unwrap().kind, hhh_im_protocol::ErrorKind::Disconnected, "之后可以重试");
}

// ---------- 性能（friend-im v10）：经插件 2 秒内到对方；新电脑从旧电脑补 1 万条历史 5 分钟内，内存有上限 ----------
// 按发布构建量才有意义，默认不跑：`cargo test --release --test plugin -- --ignored --nocapture`

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "量性能：cargo test --release --test plugin -- --ignored --nocapture"]
async fn ten_thousand_messages_through_the_plugin() {
    const MESSAGES: usize = 10_000;
    let env = Env::start_with(|c| {
        c.relay_per_second = 100_000;
        c.relay_per_minute = 1_000_000;
    })
    .await;
    let (alice, alice_key) = env.account("alice", "alice 的电脑").await;
    let (mut bob, _) = env.account("bob", "bob 的电脑").await;
    let dm = approved_dm(&bob.client, &alice.client).await;
    let program = host::program();

    // 两个真的插件程序之间：点发送到对方收到。
    let a_cred = plugin_computer(&env, "alice", &alice_key, "alice 的插件电脑").await;
    let a_dir = tempfile::tempdir().unwrap();
    let (a, _) = Host::spawn(&program, a_dir.path()).await;
    assert_eq!(a.open(&env.server.url(), Some(&a_cred), None).await["state"], "online");
    let mut worst = Duration::ZERO;
    for i in 0..20 {
        let started = Instant::now();
        send_text(&a, &dm, &format!("延迟 {i}")).await;
        bob.wait_text(&format!("延迟 {i}")).await;
        worst = worst.max(started.elapsed());
    }
    println!("经插件到对方：20 条里最慢 {worst:?}");
    assert!(worst < Duration::from_secs(2));

    // 灌 1 万条（bob 的电脑发，alice 的插件收）。
    let started = Instant::now();
    for i in 0..MESSAGES {
        bob.send_as(&dm, &friend_im_client::core::ids::MsgId::random(), text(&format!("历史 {i}"))).await.unwrap();
    }
    println!("发 {MESSAGES} 条用了 {:?}", started.elapsed());
    let raw_count = |h: &Host| h.stored(dm.as_str()).iter().filter(|m| m.raw.is_some()).count();
    let deadline = Instant::now() + Duration::from_secs(300);
    while raw_count(&a) < MESSAGES + 20 {
        assert!(Instant::now() < deadline, "alice 收不齐：{}", raw_count(&a));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // alice 的一台新电脑：插件上线后从旧电脑补齐，量时间与插件程序的内存。
    let c_cred = plugin_computer(&env, "alice", &alice_key, "alice 的新电脑").await;
    let c_dir = tempfile::tempdir().unwrap();
    let (c, _) = Host::spawn(&program, c_dir.path()).await;
    let pid = c.child.as_ref().and_then(tokio::process::Child::id).unwrap();
    let started = Instant::now();
    assert_eq!(c.open(&env.server.url(), Some(&c_cred), None).await["state"], "online");
    let at_start = resident_bytes(pid);
    tokio::time::sleep(Duration::from_millis(500)).await;
    // 补齐途中发一条：收发不受影响。
    let during = Instant::now();
    bob.send_as(&dm, &friend_im_client::core::ids::MsgId::random(), text("补齐途中")).await.unwrap();
    c.wait_text("补齐途中").await;
    println!("补齐途中的新消息到新电脑：{:?}（已补 {} 条）", during.elapsed(), raw_count(&c));
    assert!(during.elapsed() < Duration::from_secs(10));
    while raw_count(&c) < MESSAGES + 21 {
        assert!(started.elapsed() < Duration::from_secs(300), "5 分钟内没补齐：{}", raw_count(&c));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    let grown = resident_bytes(pid).saturating_sub(at_start);
    println!("补齐 {} 条用了 {:?}；插件内存多了 {:.1} MiB", raw_count(&c), started.elapsed(), mib(grown));
    assert!(grown < 50 << 20, "1 万条历史的常驻内存比空账号多不超过 50 MiB");
    wait_identical(&[&a, &c], &dm).await;
}
