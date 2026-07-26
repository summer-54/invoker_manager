#!/bin/bash
export INVOKER_GATE_SOCKET_ADDRESS="127.0.0.1:5252";
export SYSTEM_SOCKET_ADDRESS="127.0.0.1:5454"
export AUTH_API_URL="http://inf54.run/api"
export RUST_LOG=trace
cargo run --features mock --release
