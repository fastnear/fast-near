const fs = require('fs').promises;
const path = require('path');
const zlib = require('zlib');
const util = require('util');

const gzip = util.promisify(zlib.gzip);
const gunzip = util.promisify(zlib.gunzip);

const epochDataDir = './epoch-data';
const indexFilePath = path.join(epochDataDir, 'index.json.gz');

async function readEpochSummaries() {
    try {
        const compressedIndexData = await fs.readFile(indexFilePath);
        const indexData = await gunzip(compressedIndexData);
        const summaries = JSON.parse(indexData.toString());
        return summaries.sort((a, b) => a.epochHeight - b.epochHeight);
    } catch (error) {
        if (error.code !== 'ENOENT') {
            console.error('Error reading index.json.gz:', error);
        }
         
        return [];
    }
}

async function writeEpochSummaries(summaries) {
    const compressedSummaries = await gzip(JSON.stringify(summaries));
    await fs.writeFile(indexFilePath, compressedSummaries);
}

async function writeEpochData(epochHeight, data) {
    await fs.mkdir(epochDataDir, { recursive: true });
    const compressedData = await gzip(JSON.stringify(data));
    await fs.writeFile(
        path.join(epochDataDir, `${epochHeight}.json.gz`),
        compressedData
    );
}

module.exports = {
    readEpochSummaries,
    writeEpochSummaries,
    writeEpochData
};