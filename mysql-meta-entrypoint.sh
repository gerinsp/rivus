#!/bin/sh
set -eu

# Docker Compose may clear the image CMD when this custom entrypoint is used.
# Preserve the official MySQL image behavior when no explicit command arrives.
if [ "$#" -eq 0 ]; then
  set -- mysqld
fi

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
