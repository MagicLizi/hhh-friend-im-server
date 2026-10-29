//! 在线的电脑：每条 WebSocket 连接一个有界的发送队列。
//!
//! 推送在存储锁里、事务提交之后按顺序放进队列，所以每台电脑收到的推送和日志顺序一致；
//! 队列满了（电脑太慢）就断开它，让它重连后按序号补拉，慢电脑拖不垮服务器内存。

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use friend_im_core::ids::{DeviceId, Uid};
use friend_im_core::wire::ServerFrame;
use tokio::sync::{Notify, mpsc};

use crate::store::{Effects, Target};

/// 发给一条连接的东西。
#[derive(Debug)]
pub enum Outbound {
    /// 一帧（装箱：队列每格只占一个指针，1000 格的队列不会因为最大的那种帧而变大）。
    Frame(Box<ServerFrame>),
    /// 发完前面的就关闭连接。
    Close,
}

struct Conn {
    id: u64,
    approved: bool,
    reading: bool,
    tx: mpsc::Sender<Outbound>,
    kill: Arc<Notify>,
}

#[derive(Default)]
struct Inner {
    next_id: u64,
    by_device: HashMap<DeviceId, Vec<Conn>>,
    by_uid: HashMap<Uid, HashSet<DeviceId>>,
}

/// 在线电脑登记处。
#[derive(Default)]
pub struct Hub {
    inner: Mutex<Inner>,
}

/// 一条连接在登记处的凭据，断开时用它注销。
pub struct Registration {
    pub id: u64,
    pub device: DeviceId,
    pub uid: Uid,
}

impl Conn {
    fn send(&self, out: Outbound) {
        if self.tx.try_send(out).is_err() {
            tracing::warn!(conn = self.id, "连接的发送队列满了，断开让它重连补拉");
            self.kill.notify_one();
        }
    }
}

impl Hub {
    /// 登记一条连接。
    pub fn register(&self, uid: &Uid, device: &DeviceId, approved: bool, tx: mpsc::Sender<Outbound>, kill: Arc<Notify>) -> Registration {
        let mut inner = self.inner.lock().expect("登记处锁");
        inner.next_id += 1;
        let id = inner.next_id;
        inner.by_device.entry(device.clone()).or_default().push(Conn { id, approved, reading: false, tx, kill });
        inner.by_uid.entry(uid.clone()).or_default().insert(device.clone());
        Registration {
            id,
            device: device.clone(),
            uid: uid.clone(),
        }
    }

    /// 当前连接明确使用扩展后才订阅，旧客户端不会收到它不认识的帧。
    pub fn enable_reading(&self, registration: &Registration) {
        let mut inner = self.inner.lock().expect("登记处锁");
        if let Some(conn) = inner.by_device.get_mut(&registration.device).and_then(|conns| conns.iter_mut().find(|c| c.id == registration.id)) {
            conn.reading = true;
        }
    }

    /// 注销。
    pub fn unregister(&self, registration: &Registration) {
        let mut inner = self.inner.lock().expect("登记处锁");
        let empty = match inner.by_device.get_mut(&registration.device) {
            Some(conns) => {
                conns.retain(|c| c.id != registration.id);
                conns.is_empty()
            }
            None => true,
        };
        if empty {
            inner.by_device.remove(&registration.device);
            if let Some(devices) = inner.by_uid.get_mut(&registration.uid) {
                devices.remove(&registration.device);
                if devices.is_empty() {
                    inner.by_uid.remove(&registration.uid);
                }
            }
        }
    }

    /// 这台电脑现在是否在线。
    pub fn is_online(&self, device: &DeviceId) -> bool {
        self.inner.lock().expect("登记处锁").by_device.contains_key(device)
    }

    /// 给某条连接发一帧（回执）。
    pub fn reply(&self, registration: &Registration, frame: ServerFrame) {
        let inner = self.inner.lock().expect("登记处锁");
        if let Some(conn) = inner.by_device.get(&registration.device).and_then(|c| c.iter().find(|c| c.id == registration.id)) {
            conn.send(Outbound::Frame(Box::new(frame)));
        }
    }

    /// 关闭某条连接（排在已入队的帧后面）。
    pub fn close(&self, registration: &Registration) {
        let inner = self.inner.lock().expect("登记处锁");
        if let Some(conn) = inner.by_device.get(&registration.device).and_then(|c| c.iter().find(|c| c.id == registration.id)) {
            conn.send(Outbound::Close);
        }
    }

    /// 执行一次操作的推送。`requester` 的关闭留给调用方在回执之后再做；返回它是否要关闭。
    pub fn apply(&self, effects: &Effects, requester: Option<&Registration>) -> bool {
        let mut inner = self.inner.lock().expect("登记处锁");
        for device in &effects.approve {
            if let Some(conns) = inner.by_device.get_mut(device) {
                for conn in conns {
                    conn.approved = true;
                }
            }
        }
        for (target, push) in &effects.pushes {
            match target {
                Target::Account(uid) | Target::AccountExcept(uid, _) | Target::ReadingAccount(uid) => {
                    let Some(devices) = inner.by_uid.get(uid) else { continue };
                    let skipped = match target {
                        Target::AccountExcept(_, devices) => Some(devices),
                        Target::Account(_) | Target::ReadingAccount(_) => None,
                        Target::Device(_) => unreachable!(),
                    };
                    for device in devices {
                        if skipped.is_some_and(|list| list.iter().any(|skip| skip == device)) {
                            continue;
                        }
                        for conn in inner.by_device.get(device).into_iter().flatten().filter(|c| c.approved && (!matches!(target, Target::ReadingAccount(_)) || c.reading)) {
                            conn.send(Outbound::Frame(Box::new(ServerFrame::Push { push: push.clone() })));
                        }
                    }
                }
                Target::Device(device) => {
                    for conn in inner.by_device.get(device).into_iter().flatten() {
                        conn.send(Outbound::Frame(Box::new(ServerFrame::Push { push: push.clone() })));
                    }
                }
            }
        }
        let mut close_requester = false;
        for device in &effects.close {
            for conn in inner.by_device.get(device).into_iter().flatten() {
                if requester.is_some_and(|r| r.id == conn.id) {
                    close_requester = true;
                } else {
                    conn.send(Outbound::Close);
                }
            }
        }
        close_requester
    }

    /// 只发一条推送给某账号已批准的电脑（测试与运维用）。
    #[cfg(test)]
    pub fn push_to_account(&self, uid: &Uid, push: friend_im_core::wire::Push) {
        let effects = Effects {
            pushes: vec![(Target::Account(uid.clone()), push)],
            ..Default::default()
        };
        self.apply(&effects, None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use friend_im_core::wire::Push;

    #[tokio::test]
    async fn pushes_reach_only_approved_devices_and_full_queue_kills() {
        let hub = Hub::default();
        let uid = Uid::random();
        let (a, b) = (DeviceId::random(), DeviceId::random());
        let (tx_a, mut rx_a) = mpsc::channel(4);
        let (tx_b, mut rx_b) = mpsc::channel(1);
        let kill_b = Arc::new(Notify::new());
        let reg_a = hub.register(&uid, &a, true, tx_a, Arc::new(Notify::new()));
        let _reg_b = hub.register(&uid, &b, false, tx_b, kill_b.clone());
        hub.push_to_account(&uid, Push::DeviceRemoved);
        assert!(matches!(rx_a.try_recv(), Ok(Outbound::Frame(_))));
        assert!(rx_b.try_recv().is_err(), "没批准的电脑收不到账号推送");
        // b 的队列只有 1 格：塞两条直发，第二条触发断开。
        let effects = Effects {
            pushes: vec![(Target::Device(b.clone()), Push::DeviceRemoved), (Target::Device(b.clone()), Push::DeviceRemoved)],
            ..Default::default()
        };
        hub.apply(&effects, None);
        tokio::time::timeout(std::time::Duration::from_secs(1), kill_b.notified()).await.unwrap();
        hub.unregister(&reg_a);
        assert!(!hub.is_online(&a));
        assert!(hub.is_online(&b));
    }

    #[tokio::test]
    async fn account_except_skips_devices_already_served_by_lan() {
        let hub = Hub::default();
        let uid = Uid::random();
        let (a, b) = (DeviceId::random(), DeviceId::random());
        let (tx_a, mut rx_a) = mpsc::channel(2);
        let (tx_b, mut rx_b) = mpsc::channel(2);
        let _reg_a = hub.register(&uid, &a, true, tx_a, Arc::new(Notify::new()));
        let _reg_b = hub.register(&uid, &b, true, tx_b, Arc::new(Notify::new()));
        hub.apply(
            &Effects { pushes: vec![(Target::AccountExcept(uid, vec![b]), Push::DeviceRemoved)], ..Default::default() },
            None,
        );
        assert!(matches!(rx_a.try_recv(), Ok(Outbound::Frame(_))));
        assert!(rx_b.try_recv().is_err());
    }

    #[tokio::test]
    async fn reading_pushes_require_approval_and_per_connection_opt_in() {
        let hub = Hub::default();
        let uid = Uid::random();
        let device = DeviceId::random();
        let (tx_old, mut old) = mpsc::channel(2);
        let (tx_new, mut new) = mpsc::channel(2);
        let (tx_pending, mut pending) = mpsc::channel(2);
        let _old = hub.register(&uid, &device, true, tx_old, Arc::new(Notify::new()));
        let current = hub.register(&uid, &device, true, tx_new, Arc::new(Notify::new()));
        let unapproved = hub.register(&uid, &DeviceId::random(), false, tx_pending, Arc::new(Notify::new()));
        hub.enable_reading(&current);
        hub.enable_reading(&unapproved);
        hub.apply(&Effects { pushes: vec![(Target::ReadingAccount(uid), Push::ReadingChanged { conv: friend_im_core::ids::ConvId::random() })], ..Default::default() }, None);
        assert!(matches!(new.try_recv(), Ok(Outbound::Frame(_))));
        assert!(old.try_recv().is_err(), "同一设备的旧连接也不能收到新帧");
        assert!(pending.try_recv().is_err(), "订阅不绕过批准");
    }
}
