const NEAR_PROVIDER_URL = 'https://archival-rpc.mainnet.near.org';

async function getEpochValidators(blockHeight) {
    const requestOptions = {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            "jsonrpc": "2.0",
            "id": "dontcare",
            "method": "validators",
            "params": [blockHeight]
        })
    };

    try {
        const response = await fetch(NEAR_PROVIDER_URL, requestOptions);
        const data = await response.json();
        if (data.error) {
            console.error('JSON-RPC error:', data.error);
            throw new Error(`JSON-RPC error: ${data.error.message || 'Unknown error'}`);
        }
        return data.result;
    } catch (error) {
        console.error('Error fetching validators:', error);
        return null;
    }
}
const fs = require('fs').promises;
const path = require('path');
const zlib = require('zlib');
const util = require('util');

const gzip = util.promisify(zlib.gzip);
const gunzip = util.promisify(zlib.gunzip);

async function fetchAndSaveEpochData() {
    let currentBlockHeight = null; // Start by fetching the latest block
    let epochSummaries = [];
    let lastWrittenSummary = Infinity;
    const epochDataDir = './epoch-data';
    const indexFilePath = path.join(epochDataDir, 'index.json.gz');

    // Check if index.json.gz exists and load existing data
    try {
        const compressedIndexData = await fs.readFile(indexFilePath);
        const indexData = await gunzip(compressedIndexData);
        epochSummaries = JSON.parse(indexData.toString());
        if (epochSummaries.length > 0) {
            currentBlockHeight = epochSummaries[0].epochStartHeight - 1;
            lastWrittenSummary = epochSummaries[0].epochHeight;
            console.log(`Resuming from epoch ${lastWrittenSummary}`);
        }
    } catch (error) {
        if (error.code !== 'ENOENT') {
            console.error('Error reading index.json.gz:', error);
        }
        // If file doesn't exist or there's an error, start from the beginning
    }

    while (true) {
        const validatorsInfo = await getEpochValidators(currentBlockHeight);
        if (!validatorsInfo) {
            break; 
        }

        const epochHeight = validatorsInfo.epoch_height;
        const epochStartHeight = validatorsInfo.epoch_start_height;
        const numCurrentValidators = validatorsInfo.current_validators.length;
        const numNextValidators = validatorsInfo.next_validators.length;
        const numKickedOut = validatorsInfo.prev_epoch_kickout.length;

        console.log(`Epoch ${epochHeight}:`);
        console.log(`  Start Height: ${epochStartHeight}`);
        console.log(`  Number of Current Validators: ${numCurrentValidators}`);
        console.log(`  Number of Next Validators: ${numNextValidators}`);
        console.log(`  Number of Validators Kicked Out: ${numKickedOut}`);
        console.log('---');

        // Save full epoch data (compressed)
        await fs.mkdir(epochDataDir, { recursive: true });
        const compressedValidatorsInfo = await gzip(JSON.stringify(validatorsInfo));
        await fs.writeFile(
            path.join(epochDataDir, `${epochHeight}.json.gz`),
            compressedValidatorsInfo
        );

        // Add to summaries if it's a new epoch
        if (!epochSummaries.some(summary => summary.epochHeight === epochHeight)) {
            epochSummaries.unshift({
                epochHeight,
                epochStartHeight
            });
        }

        // Write summaries every 10 epochs or when we reach epoch 1
        if (lastWrittenSummary - epochHeight >= 10 || epochHeight === 1) {
            const compressedSummaries = await gzip(JSON.stringify(epochSummaries));
            await fs.writeFile(indexFilePath, compressedSummaries);
            console.log(`Wrote summaries up to epoch ${epochHeight}`);
            lastWrittenSummary = epochHeight;
        }

        if (epochHeight === 1) { // Assuming epoch height starts from 1
            break; // Stop when we reach the first epoch
        }

        currentBlockHeight = epochStartHeight - 1;
    }

    console.log('All epoch data and summaries have been saved.');
}

fetchAndSaveEpochData();
