#!/bin/bash
set -e

trap 'killall distribkv' SIGINT

cd $(dirname $0)

killall distribkv || true
sleep 0.1

go install -v

diskvgo -path=a.db -http-addr=127.0.0.1:8080 -confile=shard.toml -shard=A &
diskvgo -path=b.db -http-addr=127.0.0.1:8081 -confile=shard.toml -shard=B &
diskvgo -path=c.db -http-addr=127.0.0.1:8082 -confile=shard.toml -shard=C &
diskvgo -path=d.db -http-addr=127.0.0.1:8083 -confile=shard.toml -shard=D &

wait
