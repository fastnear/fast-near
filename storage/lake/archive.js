const fs = require('fs');
const zlib = require('zlib');
const tar = require('tar-stream');

const FILES_PER_ARCHIVE = 5;

async function *readBlocks(dataDir, shard, startBlockNumber, endBlockNumber) {
    startBlockNumber = startBlockNumber ? Math.floor(startBlockNumber / FILES_PER_ARCHIVE) * FILES_PER_ARCHIVE : 0;

    for (let blockNumber = startBlockNumber; blockNumber < endBlockNumber; blockNumber += FILES_PER_ARCHIVE) {
        console.log('blockNumber', blockNumber, 'endBlockNumber', endBlockNumber);
        const blockHeight = normalizeBlockHeight(blockNumber);
        const [prefix1, prefix2] = blockHeight.match(/^(.{6})(.{3})/).slice(1);
        const inFolder = `${dataDir}/${shard}/${prefix1}/${prefix2}`;
        const inFile = `${inFolder}/${blockHeight}.tgz`;
        console.log('inFile', inFile);

        const extract = tar.extract();
        const gunzip = zlib.createGunzip();
        const readStream = fs.createReadStream(inFile);
        readStream.pipe(gunzip).pipe(extract);

        for await (const entry of extract) {
            // Convert entry stream into data buffer
            const data = await new Promise((resolve, reject) => {
                const chunks = [];
                entry.on('data', (chunk) => chunks.push(chunk));
                entry.on('end', () => resolve(Buffer.concat(chunks)));
                entry.on('error', reject);
            });

            const blockHeight = parseInt(entry.header.name.replace('.json', ''), 10);
            yield { data, blockHeight };
        }
        // TODO: Does readStream need to be closed?
    }
}

function normalizeBlockHeight(number) {
    return number.toString().padStart(12, '0');
}

module.exports = { readBlocks };