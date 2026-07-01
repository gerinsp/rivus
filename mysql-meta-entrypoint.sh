#!/bin/sh
set -eu

docker-entrypoint.sh "$@" &
mysql_pid=$!

term_handler() {
  kill -TERM "$mysql_pid" 2>/dev/null || true
  wait "$mysql_pid"
}

trap term_handler INT TERM

export META_MYSQL_HOST=127.0.0.1
sh /bootstrap/mysql-bootstrap-user.sh

wait "$mysql_pid"
