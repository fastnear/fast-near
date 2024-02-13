#!/bin/sh
set -ex

# Try importing small chunk of real mainnet data
node scripts/load-from-near-lake.js near-lake-data-mainnet --batch-size 10 --history-length 1 --start-block-height 66377251 --limit 5 --dump-changes

# Make sure different key types supported
node scripts/load-from-near-lake.js near-lake-data-mainnet --batch-size 10 --history-length 1 --start-block-height 71745488 --limit 5 --dump-changes

# Import NEAR Lake data into ./lake-data
node scripts/load-raw-near-lake.js near-lake-data-mainnet 66377251 5

# Index NEAR Lake data
node scripts/build-raw-near-lake-index.js near-lake-data-mainnet 66377251 5

# Fail if index not created
if [ ! -f ./lake-data/near-lake-data-mainnet/0/index/changes.dat ]; then
  echo "Expected ./lake-data/near-lake-data-mainnet/0/index/changes.dat to be created"
  exit 1
fi

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
