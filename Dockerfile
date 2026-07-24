FROM rust:latest

WORKDIR /app

COPY ./Cargo.toml ./
COPY ./src ./src

ENV RUST_LOG=invoker_manager=trace
ENV INVOKER_GATE_SOCKET_ADDRESS=0.0.0.0:1111
ENV SYSTEM_SOCKET_ADDRESS=0.0.0.0:2222
ENV AUTH_API_URL=0.0.0.0:2222/api
ENV RUST_BACKTRACE=1

RUN ["apt-get", "update"]
RUN ["apt-get", "install", "clang", "--no-install-recommends", "-y"]

RUN ["cargo", "build", "--release"]

CMD ["cargo", "run", "--release"]
