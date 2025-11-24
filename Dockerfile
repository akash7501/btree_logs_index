# Use latest stable Rust
FROM rust:1.85 AS builder

WORKDIR /usr/src/app
COPY . .

RUN cargo build --release

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /usr/src/app/target/release/search /app/search
COPY --from=builder /usr/src/app/target/release/main /app/main

CMD ["./main"]
