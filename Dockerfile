# 两阶段：先在 Linux 上编译并跑完服务器的全部测试（含端到端），运行镜像里只放一个程序。
#
# 共用的格式与校验（core）和测试用的客户端库在插件仓库里，通过名为 friend-im 的构建上下文带进来
# （compose.yaml 的 additional_contexts；默认是同级目录 ../hhh-friend-im，也可以是带标签的 Git 地址）。
ARG RUST_IMAGE=rust:1-slim-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM ${RUST_IMAGE} AS build
WORKDIR /src
COPY --from=friend-im core hhh-friend-im/core
COPY --from=friend-im client hhh-friend-im/client
COPY Cargo.toml Cargo.lock hhh-friend-im-server/
COPY src hhh-friend-im-server/src
COPY tests hhh-friend-im-server/tests
WORKDIR /src/hhh-friend-im-server
RUN cargo test --release --locked && cargo build --release --locked --bin friend-im-server

FROM ${RUNTIME_IMAGE} AS runtime
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin friendim \
    && mkdir -p /data && chown friendim:friendim /data
COPY --from=build /src/hhh-friend-im-server/target/release/friend-im-server /usr/local/bin/friend-im-server
USER friendim
ENV FRIEND_IM_LISTEN=0.0.0.0:8080 FRIEND_IM_DATA_DIR=/data RUST_LOG=info
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["friend-im-server", "healthcheck"]
CMD ["friend-im-server"]
