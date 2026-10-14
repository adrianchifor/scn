FROM rust:1.99.0-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl git && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 10001 scn && mkdir /data && chown scn:scn /data
COPY --from=build /build/target/release/scn /usr/local/bin/scn
USER scn
WORKDIR /data
EXPOSE 7878
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=12 CMD curl -fsS http://127.0.0.1:7878/health || exit 1
ENTRYPOINT ["scn"]
CMD ["--help"]
