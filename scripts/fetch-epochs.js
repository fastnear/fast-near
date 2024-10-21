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

    const response = await fetch(NEAR_PROVIDER_URL, requestOptions);
    const data = await response.json();
    if (data.error) {
        console.error('JSON-RPC error:', data.error);
        const error = new Error(`JSON-RPC error: ${data.error.message || 'Unknown error'}`);
        error.data = data.error;
        throw error;
    }
    return data.result;
}

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

const EPOCH_CHECK_INTERVAL_MS = 15 * 1000;

async function fetchAndSaveEpochData() {
    let currentBlockHeight = null; // Start by fetching the latest block
    let epochSummaries = await readEpochSummaries();
    let lastWrittenSummary = Infinity;

    if (epochSummaries.length > 0) {
        currentBlockHeight = epochSummaries[0].epochStartHeight - 1;
        lastWrittenSummary = epochSummaries[0].epochHeight;
        console.log(`Resuming from epoch ${lastWrittenSummary}`);
    }

    // TODO: Handle epoch 0 (needs special handling)
    let fetchUntilEpoch = 1;

    while (true) {
        if (lastWrittenSummary === 1) {
            // We've reached epoch 1, now update fetchUntilEpoch to the latest known epoch
            fetchUntilEpoch = Math.max(...epochSummaries.map(summary => summary.epochHeight));
            currentBlockHeight = null; // Reset to fetch the latest block
            console.log('History complete, fetching until epoch', fetchUntilEpoch);
        } else {
            // We've reached the latest known epoch, wait for a while before checking for new epochs
            console.log('Waiting for new epoch...');
            await new Promise(resolve => setTimeout(resolve, EPOCH_CHECK_INTERVAL_MS));
            currentBlockHeight = null; // Reset to fetch the latest block
        }

        while (true) {
            let validatorsInfo;
            try {
                validatorsInfo = await getEpochValidators(currentBlockHeight);
            } catch (error) {
                if (error?.data?.cause?.name === 'UNKNOWN_EPOCH') {
                    console.log('Trying to skip block', currentBlockHeight);
                    currentBlockHeight -= 1;
                    continue;
                }

                throw error;
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

            // Save full epoch data
            await writeEpochData(epochHeight, validatorsInfo);

            // Add to summaries if it's a new epoch
            if (!epochSummaries.some(summary => summary.epochHeight === epochHeight)) {
                epochSummaries.unshift({
                    epochHeight,
                    epochStartHeight
                });
            }

            // Write summaries every 10 epochs or when we reach fetchUntilEpoch
            if (lastWrittenSummary - epochHeight >= 10 || epochHeight === fetchUntilEpoch) {
                await writeEpochSummaries(epochSummaries);
                console.log(`Wrote summaries up to epoch ${epochHeight}`);
                lastWrittenSummary = epochHeight;
            }

            if (epochHeight === fetchUntilEpoch) {
                break; // Stop when we reach the target epoch
            }

            currentBlockHeight = epochStartHeight - 1;
        }
    }
}
fetchAndSaveEpochData();
