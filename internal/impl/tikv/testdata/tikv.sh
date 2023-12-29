#!/bin/sh
set -e

exec ./tikv-server \
    --addr 0.0.0.0:20160 \
    --status-addr 0.0.0.0:20180 \
    --advertise-addr host.docker.internal:20160 \
    --pd-endpoints host.docker.internal:2379