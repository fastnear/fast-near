#!/bin/sh
# Try importing small chunk of real mainnet data
node scripts/load-from-near-lake.js near-lake-data-mainnet --batch-size 10 --history-length 1 --start-block-height 66377251 --limit 5
node scripts/compress-history.js


