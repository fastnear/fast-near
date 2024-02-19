const fs = require('fs');
const zlib = require('zlib');
const tar = require('tar-stream');
const { pipeline } = require('node:stream/promises')

const FILES_PER_ARCHIVE = 5;
const BLOCKS_PER_LOG = 1000;

async function *readBlocks(dataDir, shard, startBlockNumber, endBlockNumber) {
    startBlockNumber = startBlockNumber ? Math.floor(startBlockNumber / FILES_PER_ARCHIVE) * FILES_PER_ARCHIVE : 0;

    const startTime = Date.now();
    for (let blockNumber = startBlockNumber; blockNumber < endBlockNumber; blockNumber += FILES_PER_ARCHIVE) {
        const blockHeight = normalizeBlockHeight(blockNumber);
        const [prefix1, prefix2] = blockHeight.match(/^(.{6})(.{3})/).slice(1);
        const inFolder = `${dataDir}/${shard}/${prefix1}/${prefix2}`;
        const inFile = `${inFolder}/${blockHeight}.tgz`;

        if (blockNumber > startBlockNumber && blockNumber % BLOCKS_PER_LOG === 0) {
            const blocksPerSecond = (blockNumber - startBlockNumber) / ((Date.now() - startTime) / 1000);
            console.log(`Reading block ${blockNumber}. Speed: ${blocksPerSecond.toFixed(2)} blocks/s. ETA: ${(endBlockNumber - blockNumber) / blocksPerSecond} s`);
        }

        const extract = tar.extract();
        const gunzip = zlib.createGunzip();
        const readStream = fs.createReadStream(inFile);
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
                yield { data, blockHeight };
            }
        });

        await pipelinePromise;
    }
}

function normalizeBlockHeight(number) {
    return number.toString().padStart(12, '0');
}

module.exports = { readBlocks };