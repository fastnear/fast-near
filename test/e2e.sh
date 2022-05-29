#!/bin/sh
set -ex

# Try importing small chunk of real mainnet data
node scripts/load-from-near-lake.js near-lake-data-mainnet --batch-size 10 --history-length 1 --start-block-height 66377251 --limit 5

# Try compressing history
node scripts/compress-history.js

# Start server
node app &

# Kill child processes on exit
trap "pkill -SIGINT -P $$" EXIT

# Use near-cli for basic JSON-RPC tests
npx near-cli state aurora --nodeUrl http://localhost:3000
(npx near-cli view aurora no-code-here {} --nodeUrl http://localhost:3000 || false) 2>&1 | grep -q CodeDoesNotExist
