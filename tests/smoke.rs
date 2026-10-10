//! 冒烟：连一个已经跑起来的服务器（比如容器里的那个），走一遍最常用的路。默认不跑。
//!
//! 服务器要认这个测试签的凭证：测试把自己的公钥写进 `FRIEND_IM_SMOKE_JWKS_DIR/{a,b}/jwks.json`，
//! 由服务器能访问的一个静态文件服务发出去，服务器的 `FRIEND_IM_CINDY_ISSUERS` 指向它（见 README「开发」）。
//!
//! `FRIEND_IM_SMOKE_URL=http://127.0.0.1:18090 FRIEND_IM_SMOKE_JWKS_DIR=<目录> cargo test --test smoke -- --ignored --nocapture`

mod common;

use std::io::Cursor;

use common::*;
use friend_im_client::core::ids::MsgId;
use friend_im_client::core::wire::DeviceStatus;
use friend_im_client::{Credentials, HistoryItem, Update};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "冒烟：要先起服务器，见文件开头"]
async fn smoke_against_a_running_server() {
    init_logs();
    let url = std::env::var("FRIEND_IM_SMOKE_URL").expect("FRIEND_IM_SMOKE_URL");
    let dir = std::path::PathBuf::from(std::env::var("FRIEND_IM_SMOKE_JWKS_DIR").expect("FRIEND_IM_SMOKE_JWKS_DIR"));
    for issuer in ["a", "b"] {
        std::fs::create_dir_all(dir.join(issuer)).unwrap();
        std::fs::write(dir.join(issuer).join("jwks.json"), jwks_json()).unwrap();
    }
    // 只用来签凭证：签发方地址与服务器配置里的一致，公钥服务由外面提供。
    let cindy = Cindy { issuers: Vec::new() };
    let run = MsgId::random();
    let connect = async |member: &str, device: &str| {
        Peer::connect(&url, device, Credentials::generate(), Some(cindy.token(&format!("{member}-{run}"), member)), None)
            .await
            .unwrap_or_else(|e| panic!("{member} 登录失败：{e}"))
    };

    let mut alice = connect("alice", "alice 的电脑").await;
    alice.wait("连上", |u| matches!(u, Update::Connected { status: DeviceStatus::Unprovisioned }).then_some(())).await;
    let recovery = alice.client.provision().await.unwrap();
    let mut bob = connect("bob", "bob 的电脑").await;
    bob.client.provision().await.unwrap();
    let dm = bob.client.add_friend(&alice.client.friend_link(false).await.unwrap()).await.unwrap();
    alice.client.send(&dm, &MsgId::random(), text("容器里的服务器你好")).await.unwrap();
    bob.wait_text("容器里的服务器你好").await;
    let data: Vec<u8> = (0..(3 * 1024 * 1024)).map(|i: u32| (i % 241) as u8).collect();
    let file = bob.client.upload(&dm, Cursor::new(data.clone()), data.len() as u64, "照片.jpg", "image/jpeg").await.unwrap();
    let mut out = Vec::new();
    alice.client.download(&file, &mut out).await.unwrap();
    assert!(out == data);

    let mut laptop = Peer::connect(&url, "alice 的笔记本", Credentials::generate(), Some(cindy.token(&format!("alice-{run}"), "alice")), None).await.unwrap();
    laptop.client.join_with_recovery(&recovery.to_groups()).await.unwrap();
    laptop.wait("可用", |u| matches!(u, Update::Activated).then_some(())).await;
    let page = laptop.client.history(&dm, None, 10).await.unwrap();
    assert!(page.items.iter().any(|i| matches!(i, HistoryItem::Message(m) if text_of(m).as_deref() == Some("容器里的服务器你好"))));
    alice.wait("新增电脑的提醒", |u| matches!(u, Update::Account { .. }).then_some(())).await;
    println!("冒烟通过：开通、加好友、收发、3 MiB 文件、恢复密钥加新电脑并补历史");
}
