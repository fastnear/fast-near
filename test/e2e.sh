#!/bin/sh
set -ex

# Try importing small chunk of real mainnet data
node scripts/load-from-near-lake.js near-lake-data-mainnet --batch-size 10 --history-length 1 --start-block-height 66377251 --limit 5

# Try compressing history
node scripts/compress-history.js

# Use near-cli for basic JSON-RPC tests
node app &
npx near-cli state aurora --nodeUrl http://localhost:3000

# Kill child processes
pkill -P $$
