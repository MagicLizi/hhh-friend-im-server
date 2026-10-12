//! 插件端到端（第三阶段，插件仓库 plan-v1 §14.5）：用 Cindy 账号登录与插件操作。真的服务器 + 真的插件，测试扮演 HHH：
//! 照 provider-protocol v13 发 `login/start`（带假 Cindy 签的令牌）、`actions/start`、`step/choose`、`action/submit`，
//! 看插件推来的步骤、提醒与结果。
//!
//! - 登录：第一台开通出示恢复密钥；新电脑出示 4 个词，旧电脑经提醒批准（输错一次再输对）、拒绝、连错 5 次作废；改用恢复密钥；
//!   都没有了重置账号；游客（没令牌）当场失败；令牌不进插件日志。
//! - 操作：我的电脑（移除、重新生成恢复密钥）、好友链接（换一条）、加好友（失效、不是链接、自己的）、新建群、群成员（群主
//!   移出）、邀请成员（群邀请链接，凭链接进群）、退出群、关于加密。
//! - 个人资料：头像只在会话里加密分发（服务器库里找不到头像地址），对方收到 `people` 事件；头像换了重新打开时再发。
//!
//! 新电脑等批准超过 10 分钟的作废在服务器测试里验（这里不改服务器的时限）。

#[path = "../../tests/common/mod.rs"]
mod common;
mod host;

use common::*;
use friend_im::credential::Credential;
use hhh_im_protocol::types::Event;
use host::{Host, Seen, events};
use serde_json::{Value, json};

const AVATAR_A1: &str = "https://avatar.cindy.test/u/alice-1.png";
const AVATAR_A2: &str = "https://avatar.cindy.test/u/alice-2.png";
const AVATAR_B: &str = "https://avatar.cindy.test/u/bob.png";

/// 起一个插件，没有凭据打开：插件回“要登录”。
async fn fresh(env: &Env) -> (Host, tempfile::TempDir) {
    let temp = tempfile::tempdir().unwrap();
    let (host, _) = Host::start(temp.path()).await;
    let opened = host.open(&env.server.url(), None, None).await;
    assert_eq!(opened["state"], "needsLogin", "{opened}");
    assert_eq!(opened["reason"], "noCredential");
    (host, temp)
}

/// 宿主用 Cindy 账号开始登录，返回流程编号。
async fn login(host: &Host, token: &str, name: &str, avatar: Option<&str>) -> String {
    let result = host
        .call(
            "login/start",
            json!({ "method": "cindy", "values": { "hostToken": token }, "hostAccount": { "name": name, "detail": "个人", "avatarUrl": avatar } }),
        )
        .await
        .expect("login/start");
    result["flowId"].as_str().unwrap().to_owned()
}

/// 插件推来过的东西有几样（之后只看这之后的）。
fn mark(host: &Host) -> usize {
    host.seen.lock().unwrap().len()
}

/// 等这个流程在 `from` 之后推来满足 `f` 的一步（登录或操作的都认）。
async fn step(host: &Host, from: usize, flow: &str, what: &str, f: impl Fn(&Value) -> bool) -> Value {
    host.wait(what, |seen| {
        seen[from.min(seen.len())..].iter().find_map(|s| match s {
            Seen::Note(m, p) if (m == "login/step" || m == "action/step") && p["flowId"] == flow && f(&p["step"]) => Some(p["step"].clone()),
            _ => None,
        })
    })
    .await
}

/// 等这个流程在 `from` 之后的结果（`login/finished` 等）。
async fn ended(host: &Host, from: usize, flow: &str, method: &str) -> Value {
    host.wait(&format!("{method}（{flow}）"), |seen| {
        seen[from.min(seen.len())..].iter().find_map(|s| match s {
            Seen::Note(m, p) if m == method && p["flowId"] == flow => Some(p.clone()),
            Seen::Note(m, p) if p["flowId"] == flow && (m.ends_with("/finished") || m.ends_with("/failed")) => panic!("{flow} 以 {m} 结束：{p}"),
            _ => None,
        })
    })
    .await
}

async fn choose(host: &Host, flow: &str, choice: &str, row: Option<&str>, selected: &[&str]) {
    host.notify("step/choose", json!({ "flowId": flow, "choice": choice, "row": row, "selected": selected })).await;
}

async fn submit(host: &Host, method: &str, flow: &str, values: Value) {
    host.notify(method, json!({ "flowId": flow, "values": values })).await;
}

/// 开始一项操作，返回流程编号与插件给的标题。
async fn action(host: &Host, id: &str, conversation: Option<&str>) -> (String, Option<String>) {
    let result = host.call("actions/start", json!({ "actionId": id, "conversationId": conversation })).await.expect("actions/start");
    (result["flowId"].as_str().unwrap().to_owned(), result["title"].as_str().map(str::to_owned))
}

/// 最新的一次提醒（`session/notice`）。
fn notices_in(seen: &[Seen]) -> Vec<Value> {
    seen.iter()
        .rev()
        .find_map(|s| match s {
            Seen::Note(m, p) if m == "session/notice" => Some(p["notices"].as_array().cloned().unwrap_or_default()),
            _ => None,
        })
        .unwrap_or_default()
}

/// 等最新的提醒满足 `f`。
async fn wait_notices(host: &Host, what: &str, f: impl Fn(&[Value]) -> bool) -> Vec<Value> {
    host.wait(what, |seen| Some(notices_in(seen)).filter(|n| f(n))).await
}

/// 存过的最后一份凭据。
fn saved_in(seen: &[Seen]) -> Option<Credential> {
    seen.iter().rev().find_map(|s| match s {
        Seen::Save(c) => Some(Credential::decode(c).expect("凭据读得回")),
        _ => None,
    })
}

fn saved(host: &Host) -> Credential {
    saved_in(&host.seen.lock().unwrap()).expect("存过凭据")
}

/// 一个能当成“输错”的 4 个词：把第一个词换成词表里另一个。
fn wrong(code: &str) -> String {
    let mut words: Vec<&str> = code.split_whitespace().collect();
    words[0] = if words[0] == "abandon" { "ability" } else { "abandon" };
    words.join(" ")
}

/// 新电脑开始登录，等到出示 4 个词，返回（流程，4 个词）。
async fn new_computer(host: &Host, token: &str, name: &str) -> (String, String) {
    let from = mark(host);
    let flow = login(host, token, name, None).await;
    let code = step(host, from, &flow, "出示 4 个词", |s| s["kind"] == "showText" && s["layout"] == "code").await;
    assert_eq!(code["waiting"], "等待批准…（10 分钟内有效）");
    assert_eq!(code["links"][0]["id"], "useKey");
    assert_eq!(code["links"][1]["id"], "lost");
    assert_eq!(code["text"].as_str().unwrap().split_whitespace().count(), 4);
    (flow, code["text"].as_str().unwrap().to_owned())
}

/// 旧电脑：等提醒里出现在等批准的新电脑（带“查看”、要直接弹出），返回操作编号。
async fn pending_approval(old: &Host) -> String {
    let list = wait_notices(old, "新电脑等批准的提醒", |n| n.iter().any(|x| x["id"].as_str().is_some_and(|id| id.starts_with("join-")))).await;
    let notice = list.iter().find(|x| x["id"].as_str().unwrap().starts_with("join-")).unwrap();
    assert!(notice["text"].as_str().unwrap().starts_with("新电脑「"), "{notice}");
    assert_eq!(notice["prompt"], true);
    assert_eq!(notice["action"]["label"], "查看");
    notice["action"]["id"].as_str().unwrap().to_owned()
}

// ---------- S1、S2、S3、S7、S18、S20、S21、S5、S6：登录与电脑 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cindy_login_opens_the_account_and_approves_new_computers() {
    let env = Env::start().await;
    let token = env.cindy.token("alice", "alice");

    // 游客（宿主没给令牌）：当场以不能重试的失败结束。
    let (guest, _guest_dir) = fresh(&env).await;
    let from = mark(&guest);
    let flow = guest.call("login/start", json!({ "method": "cindy" })).await.unwrap()["flowId"].as_str().unwrap().to_owned();
    let failed = ended(&guest, from, &flow, "login/failed").await;
    assert_eq!(failed["retryable"], false);
    assert!(failed["message"].as_str().unwrap().contains("先登录 Cindy"));

    // S1 第一台：真的插件程序（日志经 log/write 交给宿主，查令牌不进日志）。开通 → 先存凭据 → 出示恢复密钥（不能跳过）→ 完成。
    let a1_dir = tempfile::tempdir().unwrap();
    let (a1, init) = Host::spawn(&host::program(), a1_dir.path()).await;
    assert_eq!(init["loginMethods"][0]["kind"], "host");
    assert_eq!(init["actions"].as_array().unwrap().len(), 8);
    assert_eq!(a1.open(&env.server.url(), None, None).await["state"], "needsLogin");
    let from = mark(&a1);
    let flow = login(&a1, &token, "alice", Some(AVATAR_A1)).await;
    let key_step = step(&a1, from, &flow, "出示恢复密钥", |s| s["kind"] == "showText" && s["layout"] == "words").await;
    assert_eq!(key_step["noCancel"], true);
    assert_eq!(key_step["ack"], "我已保存恢复密钥");
    let recovery = key_step["text"].as_str().unwrap().to_owned();
    assert_eq!(recovery.split_whitespace().count(), 12);
    {
        let seen = a1.seen.lock().unwrap();
        let saved_at = seen.iter().position(|s| matches!(s, Seen::Save(_))).expect("开通后先存了凭据");
        let key_at = seen.iter().position(|s| matches!(s, Seen::Note(m, p) if m == "login/step" && p["step"]["layout"] == "words")).unwrap();
        assert!(saved_at < key_at, "先存凭据再出示恢复密钥");
    }
    choose(&a1, &flow, "confirm", None, &[]).await;
    let done = ended(&a1, from, &flow, "login/finished").await;
    assert_eq!(done["title"], "Friend-IM 已开通");
    assert_eq!(done["account"]["name"], "alice");
    assert_eq!(done["account"]["avatar"], AVATAR_A1, "自己的头像键就是 Cindy 头像地址");
    assert_eq!(saved(&a1).avatar.as_deref(), Some(AVATAR_A1), "头像记进凭据");
    a1.call("conversations/list", json!({ "limit": 10 })).await.expect("登录完就在线");

    // S2、S21 新电脑：出示 4 个词；旧电脑收到要直接弹出的提醒，经“查看”批准：先输错一次（两边都有提示），再输对。
    let (a2, _a2_dir) = fresh(&env).await;
    let a2_from = mark(&a2);
    let (a2_flow, code) = new_computer(&a2, &token, "alice").await;
    let approve_id = pending_approval(&a1).await;
    let from = mark(&a1);
    let (approve_flow, title) = action(&a1, &approve_id, None).await;
    assert_eq!(title.as_deref(), Some("批准新电脑"));
    let input = step(&a1, from, &approve_flow, "批准的输入框", |s| s["kind"] == "input").await;
    assert_eq!((input["submitLabel"].as_str(), input["cancelLabel"].as_str(), input["cancelChoice"].as_str()), (Some("批准"), Some("拒绝"), Some("reject")));
    let from = mark(&a1);
    submit(&a1, "action/submit", &approve_flow, json!({ "words": wrong(&code) })).await;
    step(&a1, from, &approve_flow, "输错后的提示", |s| s["kind"] == "input" && s["note"]["text"].as_str().is_some_and(|t| t.contains("还能试 4 次"))).await;
    step(&a2, a2_from, &a2_flow, "新电脑上的输错提示", |s| s["layout"] == "code" && s["note"]["tone"] == "warn").await;
    let from = mark(&a1);
    submit(&a1, "action/submit", &approve_flow, json!({ "words": code })).await;
    let approved = ended(&a1, from, &approve_flow, "action/finished").await;
    assert!(approved["title"].as_str().unwrap().starts_with("已批准「"), "{approved}");
    let joined = ended(&a2, a2_from, &a2_flow, "login/finished").await;
    assert_eq!(joined["title"], "这台电脑已加入");
    wait_notices(&a1, "批准后提醒拿掉、多一条“新增了电脑”", |n| {
        !n.iter().any(|x| x["id"].as_str().unwrap().starts_with("join-"))
            && n.iter().any(|x| x["text"].as_str().unwrap().starts_with("你的账号新增了电脑") && x["action"]["id"] == "devices")
    })
    .await;

    // S3 改用恢复密钥：输错一次带警示，输对加入。
    let (a3, _a3_dir) = fresh(&env).await;
    let a3_from = mark(&a3);
    let (a3_flow, _) = new_computer(&a3, &token, "alice").await;
    choose(&a3, &a3_flow, "useKey", None, &[]).await;
    step(&a3, a3_from, &a3_flow, "恢复密钥输入框", |s| s["kind"] == "input" && s["submitLabel"] == "加入").await;
    let from = mark(&a3);
    submit(&a3, "login/submit", &a3_flow, json!({ "key": "abandon ability able about above absent absorb abstract absurd abuse access accident" })).await;
    step(&a3, from, &a3_flow, "恢复密钥不对", |s| s["kind"] == "input" && s["note"]["tone"] == "warn").await;
    submit(&a3, "login/submit", &a3_flow, json!({ "key": recovery })).await;
    assert_eq!(ended(&a3, a3_from, &a3_flow, "login/finished").await["title"], "这台电脑已加入");
    let a3_device = saved(&a3).device.device.id().to_string();

    // S20 拒绝：旧电脑点“拒绝”，两边以失败结束（新电脑能重新请求）。
    let (a4, _a4_dir) = fresh(&env).await;
    let a4_from = mark(&a4);
    let (a4_flow, _) = new_computer(&a4, &token, "alice").await;
    let id = pending_approval(&a1).await;
    let from = mark(&a1);
    let (flow, _) = action(&a1, &id, None).await;
    step(&a1, from, &flow, "批准的输入框", |s| s["kind"] == "input").await;
    choose(&a1, &flow, "reject", None, &[]).await;
    let rejected = ended(&a1, from, &flow, "action/failed").await;
    assert_eq!((rejected["title"].as_str(), rejected["retryable"].as_bool()), (Some("已拒绝"), Some(false)));
    let failed = ended(&a4, a4_from, &a4_flow, "login/failed").await;
    assert_eq!((failed["title"].as_str(), failed["retryable"].as_bool(), failed["retryLabel"].as_str()), (Some("没有批准"), Some(true), Some("重新请求")));

    // 连错 5 次作废。
    let (a5, _a5_dir) = fresh(&env).await;
    let a5_from = mark(&a5);
    let (a5_flow, code) = new_computer(&a5, &token, "alice").await;
    let id = pending_approval(&a1).await;
    let from = mark(&a1);
    let (flow, _) = action(&a1, &id, None).await;
    step(&a1, from, &flow, "批准的输入框", |s| s["kind"] == "input").await;
    for left in (1..=4).rev() {
        let from = mark(&a1);
        submit(&a1, "action/submit", &flow, json!({ "words": wrong(&code) })).await;
        step(&a1, from, &flow, "输错的提示", |s| s["note"]["text"].as_str().is_some_and(|t| t.contains(&format!("还能试 {left} 次")))).await;
    }
    let from = mark(&a1);
    submit(&a1, "action/submit", &flow, json!({ "words": wrong(&code) })).await;
    assert_eq!(ended(&a1, from, &flow, "action/failed").await["title"], "请求已作废");
    assert_eq!(ended(&a5, a5_from, &a5_flow, "login/failed").await["title"], "请求已作废");
    wait_notices(&a1, "作废后提醒拿掉", |n| !n.iter().any(|x| x["id"].as_str().unwrap().starts_with("join-"))).await;

    // S5 我的电脑：本机标“本机”、别的能移除；打开就拿掉电脑增减的提醒。移除 a3：先确认，回到列表带提示，a3 下线要重新登录。
    let from = mark(&a1);
    let (flow, _) = action(&a1, "devices", None).await;
    let list = step(&a1, from, &flow, "我的电脑", |s| s["kind"] == "list").await;
    let rows = list["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3, "a1、a2、a3（a4、a5 没加入）：{list}");
    let mine = rows.iter().find(|r| r["tag"] == "本机").expect("本机");
    assert!(mine.get("actions").is_none(), "本机不能移除");
    assert!(rows.iter().all(|r| r["icon"] == "device" && r["detail"].as_str().unwrap().starts_with("加入于 ")));
    assert_eq!(list["links"][0]["id"], "regen");
    wait_notices(&a1, "看过我的电脑，电脑增减的提醒拿掉", |n| !n.iter().any(|x| x["id"].as_str().unwrap().starts_with("device-"))).await;
    let from = mark(&a1);
    choose(&a1, &flow, "remove", Some(&a3_device), &[]).await;
    let confirm = step(&a1, from, &flow, "移除的确认", |s| s["kind"] == "confirm").await;
    assert_eq!((confirm["danger"].as_bool(), confirm["cancelChoice"].as_str()), (Some(true), Some("back")));
    let from = mark(&a1);
    choose(&a1, &flow, "confirm", None, &[]).await;
    let list = step(&a1, from, &flow, "移除后回到列表", |s| s["kind"] == "list" && s["note"]["text"].as_str().is_some_and(|t| t.starts_with("已移除「"))).await;
    assert_eq!(list["rows"].as_array().unwrap().len(), 2);
    let expired = a3.wait_note("session/expired", |_| true).await;
    assert_eq!(expired["reason"], "revoked");

    // S6 重新生成恢复密钥：确认 → 出示新的（不能跳过）→ 完成 → 回到列表带提示。
    let from = mark(&a1);
    choose(&a1, &flow, "regen", None, &[]).await;
    step(&a1, from, &flow, "重新生成的确认", |s| s["kind"] == "confirm" && s["confirmLabel"] == "重新生成").await;
    let from = mark(&a1);
    choose(&a1, &flow, "confirm", None, &[]).await;
    let new_key = step(&a1, from, &flow, "新的恢复密钥", |s| s["layout"] == "words").await;
    assert_ne!(new_key["text"], recovery.as_str());
    let from = mark(&a1);
    choose(&a1, &flow, "confirm", None, &[]).await;
    step(&a1, from, &flow, "回到列表", |s| s["kind"] == "list" && s["note"]["text"] == "新的恢复密钥已生效，旧的已经失效。").await;
    a1.notify("action/cancel", json!({ "flowId": flow })).await;

    // S7 都没有了：重置账号（三段后果、危险）→ 新的恢复密钥 → 完成。
    let (a6, _a6_dir) = fresh(&env).await;
    let a6_from = mark(&a6);
    let (a6_flow, _) = new_computer(&a6, &token, "alice").await;
    choose(&a6, &a6_flow, "lost", None, &[]).await;
    let reset = step(&a6, a6_from, &a6_flow, "重置的确认", |s| s["kind"] == "confirm").await;
    assert_eq!((reset["danger"].as_bool(), reset["sections"].as_array().map(Vec::len)), (Some(true), Some(3)));
    choose(&a6, &a6_flow, "confirm", None, &[]).await;
    step(&a6, a6_from, &a6_flow, "重置后的恢复密钥", |s| s["layout"] == "words").await;
    choose(&a6, &a6_flow, "confirm", None, &[]).await;
    assert_eq!(ended(&a6, a6_from, &a6_flow, "login/finished").await["title"], "账号已重置");

    // 令牌只在 login/start 里交一次：插件交给宿主的日志与通知里都没有它。
    let text = a1.notes_text();
    assert!(text.contains("log/write"), "真的插件程序把日志交给宿主");
    assert!(!text.contains(&token), "令牌进了插件日志");
    assert!(a1.bad_lines.lock().unwrap().is_empty());
    a1.close().await;
}

// ---------- S8、S9、S11、S12、S13、S17 与个人资料 ----------

/// 一个用 Cindy 账号开通好的插件（第一台电脑），返回插件与它的目录。
async fn opened(env: &Env, member: &str, avatar: Option<&str>) -> (Host, tempfile::TempDir) {
    let (host, dir) = fresh(env).await;
    let from = mark(&host);
    let flow = login(&host, &env.cindy.token(member, member), member, avatar).await;
    step(&host, from, &flow, "出示恢复密钥", |s| s["layout"] == "words").await;
    choose(&host, &flow, "confirm", None, &[]).await;
    ended(&host, from, &flow, "login/finished").await;
    (host, dir)
}

/// 我的好友链接一步里的链接。
async fn my_link(host: &Host) -> (String, String) {
    let from = mark(host);
    let (flow, _) = action(host, "link", None).await;
    let shown = step(host, from, &flow, "好友链接", |s| s["kind"] == "showText" && s["layout"] == "line").await;
    assert_eq!(shown["copy"], true);
    (flow, shown["text"].as_str().unwrap().to_owned())
}

/// 加好友：粘贴链接 → 核对后的确认 → 加好友，返回成功的标题。
async fn add_friend(host: &Host, link: &str) -> Value {
    let from = mark(host);
    let (flow, _) = action(host, "add", None).await;
    step(host, from, &flow, "加好友的输入框", |s| s["kind"] == "input").await;
    let from = mark(host);
    submit(host, "action/submit", &flow, json!({ "link": link })).await;
    let confirm = step(host, from, &flow, "加好友的确认", |s| s["kind"] == "confirm").await;
    assert!(confirm["person"]["detail"].as_str().unwrap().starts_with("身份指纹 "), "{confirm}");
    choose(host, &flow, "confirm", None, &[]).await;
    ended(host, from, &flow, "action/finished").await
}

/// 推来过的某种会话（按名字）。
async fn conversation_named(host: &Host, name: &str) -> String {
    host.wait(&format!("会话「{name}」"), |seen| {
        events(seen).into_iter().find_map(|e| match e {
            Event::Conversation { conversation } if conversation.name == name => Some(conversation.id),
            _ => None,
        })
    })
    .await
}

/// 推来过的这个人的头像（最新的一次）。
fn avatar_in(seen: &[Seen], uid: &str) -> Option<Option<String>> {
    events(seen).into_iter().rev().find_map(|e| match e {
        Event::People { people } => people.into_iter().find(|p| p.id == uid).map(|p| p.avatar),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn friends_groups_and_profiles_through_plugin_actions() {
    let env = Env::start().await;
    let (alice, _a_dir) = opened(&env, "alice", Some(AVATAR_A1)).await;
    let (bob, _b_dir) = opened(&env, "bob", Some(AVATAR_B)).await;
    let (carol, _c_dir) = opened(&env, "carol", None).await;
    let alice_uid = saved(&alice).uid;
    let bob_uid = saved(&bob).uid;

    // S8 好友链接：换一条（先确认），旧的失效、提示写明。
    let (flow, old_link) = my_link(&alice).await;
    let from = mark(&alice);
    choose(&alice, &flow, "renew", None, &[]).await;
    step(&alice, from, &flow, "换链接的确认", |s| s["kind"] == "confirm" && s["confirmLabel"] == "换一条").await;
    let from = mark(&alice);
    choose(&alice, &flow, "confirm", None, &[]).await;
    let renewed = step(&alice, from, &flow, "新链接", |s| s["layout"] == "line" && s["note"]["text"].as_str().is_some_and(|t| t.starts_with("已换成新链接"))).await;
    let link = renewed["text"].as_str().unwrap().to_owned();
    assert_ne!(link, old_link);
    alice.notify("action/cancel", json!({ "flowId": flow })).await;

    // S9 加好友：失效的、不是链接的、自己的都带提示回到输入框；对的核对身份后添加，私聊出现在两边。
    let from = mark(&bob);
    let (flow, _) = action(&bob, "add", None).await;
    step(&bob, from, &flow, "加好友的输入框", |s| s["kind"] == "input").await;
    for (text, expect) in [(old_link.as_str(), "已经失效"), ("随便写的", "这不是好友链接")] {
        let from = mark(&bob);
        submit(&bob, "action/submit", &flow, json!({ "link": text })).await;
        step(&bob, from, &flow, expect, |s| s["kind"] == "input" && s["note"]["text"].as_str().is_some_and(|t| t.contains(expect))).await;
    }
    let (bob_link_flow, bob_link) = my_link(&bob).await;
    bob.notify("action/cancel", json!({ "flowId": bob_link_flow })).await;
    let (flow, _) = action(&bob, "add", None).await;
    let from = mark(&bob);
    submit(&bob, "action/submit", &flow, json!({ "link": bob_link })).await;
    step(&bob, from, &flow, "自己的链接", |s| s["note"]["text"] == "这是你自己的好友链接。").await;
    bob.notify("action/cancel", json!({ "flowId": flow })).await;
    let added = add_friend(&bob, &link).await;
    assert_eq!(added["title"], "已添加alice");
    conversation_named(&alice, "bob").await;
    conversation_named(&bob, "alice").await;
    let (flow, _) = action(&carol, "link", None).await;
    carol.notify("action/cancel", json!({ "flowId": flow })).await;
    add_friend(&carol, &link).await;

    // 个人资料：加好友后两边各发一条，对方收到 people 事件带头像；carol 没有头像。服务器库里找不到头像地址。
    alice.wait("bob 的头像", |seen| avatar_in(seen, &bob_uid).flatten().filter(|a| a == AVATAR_B)).await;
    bob.wait("alice 的头像", |seen| avatar_in(seen, &alice_uid).flatten().filter(|a| a == AVATAR_A1)).await;
    let members = bob.call("people/get", json!({ "ids": [alice_uid] })).await.unwrap();
    assert_eq!(members["found"][0]["avatar"], AVATAR_A1);
    let stored = std::fs::read(env.server.dir.path().join("friend-im.sqlite3")).unwrap();
    let wal = std::fs::read(env.server.dir.path().join("friend-im.sqlite3-wal")).unwrap_or_default();
    for bytes in [&stored, &wal] {
        assert!(!bytes.windows(b"avatar.cindy.test".len()).any(|w| w == b"avatar.cindy.test"), "服务器库里有头像地址的明文");
    }
    let refused = alice.call("avatars/read", json!({ "key": "https://evil.example.org/x.png", "offset": 0, "length": 1024 })).await;
    assert!(refused.is_err(), "不在名单上的主机不取");

    // S11 新建群：群名空着带提示；勾选好友后创建。
    let from = mark(&alice);
    let (flow, _) = action(&alice, "group", None).await;
    step(&alice, from, &flow, "群名", |s| s["kind"] == "input").await;
    let from = mark(&alice);
    submit(&alice, "action/submit", &flow, json!({ "name": "  " })).await;
    step(&alice, from, &flow, "群名空着", |s| s["note"]["text"] == "群名不能是空的。").await;
    let from = mark(&alice);
    submit(&alice, "action/submit", &flow, json!({ "name": "周末爬山" })).await;
    let pick = step(&alice, from, &flow, "选好友", |s| s["kind"] == "list").await;
    assert_eq!(pick["select"]["confirmLabel"], "创建");
    let ids: Vec<String> = pick["rows"].as_array().unwrap().iter().map(|r| r["id"].as_str().unwrap().to_owned()).collect();
    assert_eq!(ids.len(), 2, "好友是 bob 与 carol：{pick}");
    let selected: Vec<&str> = ids.iter().map(String::as_str).collect();
    choose(&alice, &flow, "confirm", None, &selected).await;
    let created = ended(&alice, from, &flow, "action/finished").await;
    assert_eq!(created["title"], "群「周末爬山」已创建");
    let group = conversation_named(&bob, "周末爬山").await;
    conversation_named(&carol, "周末爬山").await;
    assert!(bob.call("actions/start", json!({ "actionId": "members", "conversationId": conversation_named(&bob, "alice").await })).await.is_err(), "私聊没有群的操作");

    // S13 群成员：成员看不到“移出”；群主移出 bob（先确认），回到列表带提示，bob 那边会话没有了。
    let from = mark(&bob);
    let (flow, _) = action(&bob, "members", Some(&group)).await;
    let list = step(&bob, from, &flow, "成员看到的群成员", |s| s["kind"] == "list").await;
    assert!(list["caption"].as_str().unwrap().contains("只有群主alice能移出成员"), "{list}");
    assert!(list["rows"].as_array().unwrap().iter().all(|r| r.get("actions").is_none()));
    bob.notify("action/cancel", json!({ "flowId": flow })).await;
    let from = mark(&alice);
    let (flow, _) = action(&alice, "members", Some(&group)).await;
    let list = step(&alice, from, &flow, "群主看到的群成员", |s| s["kind"] == "list").await;
    assert!(list["caption"].as_str().unwrap().contains("你是群主"));
    assert!(list["rows"].as_array().unwrap().iter().any(|r| r["tag"] == "群主" && r["detail"] == "你自己"));
    let from = mark(&alice);
    choose(&alice, &flow, "remove", Some(&bob_uid), &[]).await;
    step(&alice, from, &flow, "移出的确认", |s| s["kind"] == "confirm" && s["danger"] == true).await;
    let from = mark(&alice);
    choose(&alice, &flow, "confirm", None, &[]).await;
    step(&alice, from, &flow, "移出后的列表", |s| s["kind"] == "list" && s["note"]["text"] == "已把「bob」移出群。").await;
    alice.notify("action/cancel", json!({ "flowId": flow })).await;
    bob.wait("bob 的群没有了", |seen| {
        events(seen).into_iter().find(|e| matches!(e, Event::ConversationRemoved { conversation_id } if *conversation_id == group))
    })
    .await;

    // S12 邀请成员：可勾选的只有不在群里的好友（bob）；改用群邀请链接，bob 在“加好友”里粘贴它进群。
    let from = mark(&alice);
    let (flow, _) = action(&alice, "invite", Some(&group)).await;
    let pick = step(&alice, from, &flow, "可邀请的好友", |s| s["kind"] == "list").await;
    let rows = pick["rows"].as_array().unwrap();
    assert_eq!((rows.len(), rows[0]["id"].as_str()), (1, Some(bob_uid.as_str())), "{pick}");
    let from = mark(&alice);
    choose(&alice, &flow, "groupLink", None, &[]).await;
    let shown = step(&alice, from, &flow, "群邀请链接", |s| s["kind"] == "showText").await;
    let group_link = shown["text"].as_str().unwrap().to_owned();
    alice.notify("action/cancel", json!({ "flowId": flow })).await;
    let from = mark(&bob);
    let (flow, _) = action(&bob, "add", None).await;
    submit(&bob, "action/submit", &flow, json!({ "link": group_link })).await;
    step(&bob, from, &flow, "进群的确认", |s| s["kind"] == "confirm" && s["confirmLabel"] == "加入").await;
    choose(&bob, &flow, "confirm", None, &[]).await;
    assert_eq!(ended(&bob, from, &flow, "action/finished").await["title"], "已加入群「周末爬山」");
    alice.wait_text("bob 通过群邀请链接加入了群").await;

    // 退出群：确认后直接关（不给文字），carol 那边会话没有了，群里有提示。
    let from = mark(&carol);
    let (flow, _) = action(&carol, "leave", Some(&group)).await;
    step(&carol, from, &flow, "退群的确认", |s| s["kind"] == "confirm" && s["confirmLabel"] == "退出群").await;
    choose(&carol, &flow, "confirm", None, &[]).await;
    let left = ended(&carol, from, &flow, "action/finished").await;
    assert!(left.get("title").is_none_or(Value::is_null) && left.get("message").is_none_or(Value::is_null), "{left}");
    alice.wait_text("carol 退出了群").await;

    // S17 关于加密：只有“知道了”，不能跳过。
    let from = mark(&alice);
    let (flow, _) = action(&alice, "about", None).await;
    let about = step(&alice, from, &flow, "关于加密", |s| s["kind"] == "confirm").await;
    assert_eq!((about["noCancel"].as_bool(), about["sections"].as_array().map(Vec::len)), (Some(true), Some(4)));
    choose(&alice, &flow, "confirm", None, &[]).await;
    ended(&alice, from, &flow, "action/finished").await;

    // 头像换了（凭据里的和发过的不一样）：重新打开时往每个会话再发一次，bob 收到新头像。
    let mut credential = saved(&alice);
    assert_eq!(credential.announced.as_ref().and_then(|a| a.avatar.as_deref()), Some(AVATAR_A1), "发完记下了");
    credential = credential.with_avatar(Some(AVATAR_A2));
    alice.close().await;
    let dir = tempfile::tempdir().unwrap();
    let (again, _) = Host::start(dir.path()).await;
    assert_eq!(again.open(&env.server.url(), Some(&credential.encode()), None).await["state"], "online");
    bob.wait("alice 的新头像", |seen| avatar_in(seen, &alice_uid).flatten().filter(|a| a == AVATAR_A2)).await;
    again.wait("记下新头像发过了", |seen| saved_in(seen).filter(|c| c.announced.as_ref().and_then(|a| a.avatar.as_deref()) == Some(AVATAR_A2))).await;
}
