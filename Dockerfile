# Stage 1: Build Rust binaries
FROM rust:1.85 AS builder

WORKDIR /usr/src/app

# Copy all sources
COPY . .

# Build ALL binaries in release mode
RUN cargo build --release

# Stage 2: Runtime image
FROM debian:bookworm-slim

WORKDIR /app

# Copy both binaries from builder
COPY --from=builder /usr/src/app/target/release/main /app/main
COPY --from=builder /usr/src/app/target/release/search /app/search

# Make binaries executable
RUN chmod +x /app/main /app/search

# Default command: run the main binary
CMD ["./main"]
