#!/bin/sh
set -ex

# Try importing small chunk of real mainnet data
node scripts/load-from-near-lake.js near-lake-data-mainnet --batch-size 10 --history-length 1 --start-block-height 66377251 --limit 5 --dump-redis

# Make sure different key types supported
node scripts/load-from-near-lake.js near-lake-data-mainnet --batch-size 10 --history-length 1 --start-block-height 71745488 --limit 5 --dump-redis

# Try compressing history
node scripts/compress-history.js

# Start server
bin/fast-near &

# Kill child processes on exit
trap "pkill -SIGINT -P $$" EXIT

# Use near-cli for basic JSON-RPC tests
export NODE_ENV=mainnet
npx near-cli state aurora --nodeUrl http://localhost:3000
(npx near-cli view aurora no-code-here {} --nodeUrl http://localhost:3000 || false) 2>&1 | grep -q CodeDoesNotExist
(npx near-cli view no-such-account some-method {} --nodeUrl http://localhost:3000 || false) 2>&1 | grep -q "Account no-such-account is not found in mainnet"
