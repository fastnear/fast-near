const fs = require('fs');
const zlib = require('zlib');
const tar = require('tar-stream');
const { pipeline } = require('node:stream/promises')

const debug = require('debug')('source:lake');

const FILES_PER_ARCHIVE = 5;
const BLOCKS_PER_LOG = 1000;

const MAX_BLOCKS_SKIPPED = parseInt(process.env.FAST_NEAR_MAX_BLOCKS_SKIPPED || '100');

async function *readBlocks({ dataDir, shards, startBlockHeight, endBlockHeight }) {
    debug('readBlocks', dataDir, shards, startBlockHeight, endBlockHeight);
    if (!shards.includes('block')) {
        shards = [...shards, 'block'];
    }
    // TODO: Check genesis block hash?
    let lastBlockHash
    let blocksSkipped = 0;
    for (let baseBlockHeight = Math.floor(startBlockHeight / FILES_PER_ARCHIVE) * FILES_PER_ARCHIVE; baseBlockHeight < endBlockHeight; baseBlockHeight += FILES_PER_ARCHIVE) {
        blocksSkipped += FILES_PER_ARCHIVE;
        const blocks = [...Array(FILES_PER_ARCHIVE)].map(() => ({ shards: shards.slice(0, -1).map(() => ({}))}));
        for (let i = 0; i < shards.length; i++) {
            const shard = shards[i];
            const batch = await readShardBlocksBatch({ blockNumber: baseBlockHeight, dataDir, shard });
            for (const { data, blockHeight } of batch) {
                const block = blocks[blockHeight - baseBlockHeight];
                if (shard === 'block') {
                    block.block = JSON.parse(data.toString('utf8'));
                } else {
                    block.shards[i] = JSON.parse(data.toString('utf8'));
                }
            }
        }

        // TODO: Check where else to validate / refactor
        for (let block of blocks) {
            if (block.block) {
                if (lastBlockHash && block.block.header.prev_hash !== lastBlockHash) {
                    throw new Error(`Block hash chain is broken at block ${block.block.header.height}`);
                }
                blocksSkipped = 0;
                lastBlockHash = block.block.header.hash;
                yield block;
            } else {
                blocksSkipped++;
            }
        }

        if (blocksSkipped > MAX_BLOCKS_SKIPPED) {
            throw new Error(`Skipped ${blocksSkipped} blocks in a row. Some of lake data is likely missing`);
        }
    }
}

// TODO: Update the build index script / lake storage accordingly

async function *readShardBlocks({ dataDir, shard, startBlockHeight: startBlockNumber, endBlockHeight: endBlockNumber }) {
    startBlockNumber = startBlockNumber ? Math.floor(startBlockNumber / FILES_PER_ARCHIVE) * FILES_PER_ARCHIVE : 0;
    debug('readShardBlocks', dataDir, shard, startBlockNumber, endBlockNumber);

    const startTime = Date.now();
    debug('startTime:', startTime);
    // TODO: Convert start number to base block number?
    for (let blockNumber = startBlockNumber; blockNumber < endBlockNumber; blockNumber += FILES_PER_ARCHIVE) {
        if (blockNumber > startBlockNumber && blockNumber % BLOCKS_PER_LOG === 0) {
            const blocksPerSecond = (blockNumber - startBlockNumber) / ((Date.now() - startTime) / 1000);
            console.log(`Reading block ${blockNumber}. Speed: ${blocksPerSecond.toFixed(2)} blocks/s. ETA: ${(endBlockNumber - blockNumber) / blocksPerSecond} s`);
        }

        const batch = await readShardBlocksBatch({ blockNumber, dataDir, shard });
        yield *batch;
    }
}

async function readShardBlocksBatch({ blockNumber, dataDir, shard }) {
        const blockHeight = normalizeBlockHeight(blockNumber);
        const [prefix1, prefix2] = blockHeight.match(/^(.{6})(.{3})/).slice(1);
        const inFolder = `${dataDir}/${shard}/${prefix1}/${prefix2}`;
        const inFile = `${inFolder}/${blockHeight}.tgz`;

        debug('reading block data', inFile);
        const extract = tar.extract();
        const gunzip = zlib.createGunzip();
        const readStream = fs.createReadStream(inFile);
        try {
            const results = [];
            const pipelinePromise = pipeline(readStream, gunzip, extract, async function *(extract, { signal }) {
                for await (const entry of extract) {
                    if (signal.aborted) {
                        return;
                    }

                    const data = await new Promise((resolve, reject) => {
                        const chunks = [];
                        entry.on('data', (chunk) => chunks.push(chunk));
                        entry.on('end', () => resolve(Buffer.concat(chunks)));
                        entry.on('error', reject);
                    });

                    const blockHeight = parseInt(entry.header.name.replace('.json', ''), 10);
                    results.push({ data, blockHeight });
                }
            });
            await pipelinePromise;
            return results;
        } catch (e) {
            // NOTE: It's expected that some block numbers are missing
            // However block hash chain needs to be checked for consistency
            // TODO: Figure out where best to check block hash chain besides readBlocks
            if (e.code === 'ENOENT') {
                debug('file not found', inFile);
                return [];
            }

            debug('error reading block data from', inFile, e);
            throw e;
        } finally {
            // NOTE: After analysis with why-is-node-running looks like at least Gunzip is not properly closed
            gunzip.close();
            readStream.close();
        }
}

function normalizeBlockHeight(number) {
    return number.toString().padStart(12, '0');
}

module.exports = { readBlocks, readShardBlocks };