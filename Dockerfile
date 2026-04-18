FROM rust:1.88-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/backend_service /usr/local/bin/backend_service

ENV HOST=0.0.0.0
ENV PORT=8080
ENV RUST_LOG=info

EXPOSE 8080

CMD ["backend_service"]
