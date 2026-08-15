FROM rust:1-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bin spark-runner

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 uap
WORKDIR /app
COPY --from=builder /src/target/release/spark-runner /usr/local/bin/spark-runner
COPY codex.lock ./codex.lock
COPY protocol ./protocol
USER uap
EXPOSE 8787
ENTRYPOINT ["spark-runner", "serve", "--live"]
