# Friend-IM 服务器

[Friend-IM](https://github.com/MagicLizi/hhh-friend-im) 的服务器：存储账号、设备、好友、群的签名链，以及加密后的消息与文件。**只存密文，解不开任何内容。** 在线电脑、手机或平板在同一局域网时，客户端会先用端到端加密信封直连；服务器继续落库并只向未直连设备推送，承担离线、历史和多设备同步。

> 状态：服务器与客户端库已完成、测试齐全；插件已能按 HHH 插件协议收发（`plugin-e2e/` 验过），还不能在 Friend 里登录，暂时不能用。

## 服务器看得到什么

- **看不到**：消息内容、图片、文件内容和文件名、群名、群头像、群简介、回复的是哪一条。
- **看得到**：谁和谁有私聊、每个群有哪些成员；每条消息的发送时间和大小、撤回了哪一条；文件的大小和属于哪个会话；
  名字（来自 Cindy）；每个账号有几台电脑、电脑名、加入和最近在线的时间。登录时经手的 Cindy 凭证里的内容（账号编号等）
  核对完就丢掉，库里只留名字和账号编号的摘要。
- 服务器能做的坏事只剩扣留或拒绝服务：私自加电脑、加群成员、换钥匙，签名对不上，插件会发现并拒绝（测试见 `tests/e2e.rs` 的 `forged_members_devices_and_keys_are_rejected`）。
- 日志只记编号、长度、耗时和错误码，不记密文、钥匙、Cindy 凭证和登录凭证。

钥匙模型、签名链和加密构造见插件仓库的 [README](https://github.com/MagicLizi/hhh-friend-im) 和 `core/` 的文档注释。

## 怎么工作

- 每台设备一条 WebSocket 长连接 `GET /v1/ws`，上面跑请求 / 回执和服务器主动推送，格式全部定义在插件仓库的 `core/src/wire.rs`（服务器和插件按同一个版本编译）。
- 登录：每台设备第一次连接时交一次 Cindy 身份凭证。服务器只用 Cindy 公开的公钥核对（规则与 HHH 相同：RS256、两个签发方、公钥缓存 5 分钟），按“签发方 + membership 编号”区分账号，第一次见到就开通；核对后发自己的登录凭证（库里只存摘要），Cindy 凭证随即丢掉。手机和平板与电脑使用同一套批准和端到端密钥流程。
- 同步：每条消息、撤回、签名链变化、钥匙封装都有一个全局递增的序号；电脑在线时直接推，断线重连后按序号补拉，不漏不重。
- 历史：按序号往前翻，可带一个时间窗口（按会话 + 时间的索引）；“会话活动”只回自己所在会话的编号和一段时间里最早、最新一条消息的时间，不碰内容。
- 文件：`PUT` / `HEAD` / `GET /v1/files/{编号}` 按段上传、断点续传、按范围下载（只有密文）。
- 一致性：批准新电脑、移除电脑、重置账号、移出成员并换代这类操作，各是一个请求一个数据库事务；请求里任何一项校验不过，整批不生效。
- 存储：一个 SQLite 文件（WAL 模式）+ 一个文件目录，都在数据目录里。

## 上限

任何 Cindy 账号都能登录，所以每一项都有上限（定义在 `core/src/limits.rs`）：

| 项 | 上限 |
|---|---|
| 每个账号的设备 | 10 台 |
| 每个账号存的文件 | 2 GiB；单个文件 100 MiB |
| 单条消息密文 | 80 KiB（文字最多约 64 KiB） |
| 发送频率 | 每个账号每秒 5 条、每分钟 120 条（可配置） |
| 群人数 | 200 |
| 新设备批准请求 | 每个账号同时 1 个、每小时 10 个；10 分钟没批准作废；输错 5 次作废 |
| 每台电脑的推送队列 | 1000 条，满了断开，让它重连后按序号补拉 |
| 一帧请求 | 1 MiB（文件内容另走 HTTPS，每段 8 MiB） |

## 部署（Docker Compose）

需要 Docker Engine、**Buildx** 和 Docker Compose 插件（`docker compose version`、`docker buildx version` 都能用；也可以用 Podman：`podman build --build-context friend-im=<插件仓库> .`）。

服务器和插件共用的 `core` 在插件仓库里，所以两个仓库并排放：

```sh
git clone https://github.com/MagicLizi/hhh-friend-im.git
git clone https://github.com/MagicLizi/hhh-friend-im-server.git
cd hhh-friend-im-server
cp .env.example .env          # 按需改端口等，全部可选
docker compose up -d --build --wait
docker compose exec server friend-im-server healthcheck && echo 正常
```

- 构建时先在 Linux 上跑完全部测试（含端到端），再出只有一个程序的运行镜像。
- 只绑宿主机的 `127.0.0.1:8090`（`.env` 里的 `FRIEND_IM_PORT` 可改），对公网只开 443，由宿主机上已有的反向代理挂证书：
  Caddy 参考 `deploy/Caddyfile`，Nginx 参考 `deploy/nginx.conf`（WebSocket、8 MiB 的分段上传、不开访问日志都已配好）。
  Nginx 的证书：先只启用 80 端口那段，`certbot certonly --webroot -w /var/lib/letsencrypt/friend-im -d <域名>` 申请，
  再启用 443 那段；在 `/etc/letsencrypt/renewal-hooks/deploy/` 放一个 `nginx -t && systemctl reload nginx` 的脚本，续期后自动换上新证书。
- 程序以非 root 用户运行、根目录只读、不带任何权限，限 1 GiB 内存。
- 插件仓库也可以不放同级目录：在 `.env` 里写 `FRIEND_IM_SOURCE=https://github.com/MagicLizi/hhh-friend-im.git#<标签>`。
  在开发机上构建时用这个 Git 地址或一份干净的副本，免得把插件仓库的 `target/` 也传给构建器。
- 只运行一个服务进程（存储层持有一把锁，不支持多副本）。

更新、日志与停止：

```sh
git -C ../hhh-friend-im pull --ff-only && git pull --ff-only
docker compose up -d --build --wait
docker compose logs --tail=100 server
docker compose down        # 不要加 --volumes，数据在 volume 里
```

### 备份与恢复

数据库可以在线备份（SQLite 的 backup 接口，不用停服务）；文件目录里的文件传完后不再改，直接打包：

```sh
docker compose exec server friend-im-server backup /data/backup.sqlite3
docker compose cp server:/data/backup.sqlite3 ./friend-im-$(date +%F).sqlite3
docker compose exec server rm /data/backup.sqlite3
docker run --rm -v hhh-friend-im-server_friend_im_data:/data:ro -v "$PWD":/out debian:bookworm-slim \
  tar czf /out/friend-im-files-$(date +%F).tgz -C /data files
```

恢复：先 `docker compose down`，把备份的数据库放回 volume 里的 `friend-im.sqlite3`（同时删掉旁边的 `-wal`、`-shm`），解开文件包到 volume 的 `files/`，再 `docker compose up -d`。
volume 名按 Compose 项目名生成，用 `docker volume ls` 确认。

### 配置

全部来自环境变量，仓库里只有示例 `.env.example`，不放任何真实部署配置：

| 变量 | 默认 | 说明 |
|---|---|---|
| `FRIEND_IM_LISTEN` | `0.0.0.0:8080` | 监听地址（容器内） |
| `FRIEND_IM_DATA_DIR` | `/data` | 数据目录：数据库与文件 |
| `FRIEND_IM_SEND_PER_SECOND`、`FRIEND_IM_SEND_PER_MINUTE` | `5`、`120` | 每个账号的发送频率 |
| `FRIEND_IM_CINDY_ISSUERS` | Cindy 的两个签发方 | 只在测试或私有部署时改：`签发方=公钥地址;签发方=公钥地址` |
| `RUST_LOG` | `info` | 日志级别 |

## 开发

服务器按本机路径引用插件仓库里的 `core`（运行用）和客户端库（端到端测试用），两个仓库要并排放：

```sh
cargo test                                               # 单元测试 + 端到端（tests/e2e.rs）
cargo test --release --test perf -- --ignored --nocapture # 性能：新电脑补 1 万条历史、消息延迟
cd plugin-e2e
cargo test                                               # 插件端到端：测试扮演 HHH，和真的插件讲协议
cargo test --release -- --ignored --nocapture            # 经插件的性能：延迟、新电脑翻 1 万条历史、插件内存
```

`plugin-e2e/` 是单独的测试包（同时依赖服务器与插件仓库，镜像构建不碰它）：本机起假 Cindy 和服务器，用客户端库开通账号、
加好友、建群，把凭据交给插件，测试扮演 HHH 走一遍会话列表、收发五种内容与回复、推送确认与重推、撤回、按时间翻历史、
按段读附件与断网续读、发送去重与取消、断网与重启后补收、退出登录与被移出；另起一次真的插件程序，核对标准输出只有协议消息、
交给宿主的日志里没有正文。

端到端测试在本机起一个假 Cindy（自己生成 RSA 钥匙、发公钥、签凭证）、真的服务器和真的客户端库，按契约的场景走一遍：
开通与加电脑、恢复密钥、重置、好友与群、各种消息、断网补收、服务器重启、批准被拒或作废、Cindy 凭证核对、伪造识破，
最后直接扫数据库和文件目录，确认找不到任何明文。设 `RUST_LOG=friend_im_server=debug,friend_im_client=debug` 可看日志。

对一个已经跑起来的服务器（比如刚构建的容器）做冒烟：服务器要认测试签的凭证，所以旁边起一个静态文件服务发测试的公钥，
服务器的 `FRIEND_IM_CINDY_ISSUERS` 指向它（`<目录>` 由测试写入 `a/jwks.json`、`b/jwks.json`）：

```sh
docker network create friend-im-smoke
docker run -d --name fim-cindy --network friend-im-smoke -v <目录>:/www:ro busybox httpd -f -p 8000 -h /www
docker run -d --name fim-server --network friend-im-smoke --read-only --tmpfs /tmp --cap-drop ALL -p 127.0.0.1:18090:8080 \
  -e 'FRIEND_IM_CINDY_ISSUERS=https://cindy-a.test=http://fim-cindy:8000/a/jwks.json;https://cindy-b.test=http://fim-cindy:8000/b/jwks.json' \
  friend-im-server:local
FRIEND_IM_SMOKE_URL=http://127.0.0.1:18090 FRIEND_IM_SMOKE_JWKS_DIR=<目录> cargo test --test smoke -- --ignored --nocapture
```

## 许可证

[MIT](LICENSE)。
