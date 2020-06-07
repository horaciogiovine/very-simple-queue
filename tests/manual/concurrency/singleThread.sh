#!/usr/bin/env bash

if [[ "$1" == "redis" ]]; then
    docker run -d -p 6379:6379 redis
fi

if [[ "$1" == "sqlite3" ]]; then
  touch ./tests/manual/concurrency/db.sqlite3
fi

node --trace-warnings ./tests/manual/concurrency/createJobs.js $1
node --trace-warnings ./tests/manual/concurrency/work.js $1 workerA workerB workerC workerD