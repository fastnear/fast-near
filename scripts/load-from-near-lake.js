const { stream } = require('near-lake-framework');
const bs58 = require('bs58');
const { serialize } = require('borsh');
const { setLatestBlockHeight, setData, deleteData, cleanOlderData, redisBatch, closeRedis, setBlockTimestamp } = require('../storage-client');
const { accountKey, dataKey, codeKey } = require('../storage-keys');
const { Account, BORSH_SCHEMA } = require('../data-model');

const { withTimeCounter, getCounters, resetCounters} = require('../counters');

let totalMessages = 0;
let timeStarted = Date.now();

async function handleStreamerMessage(streamerMessage, { historyLength } = {}) {
    const { height: blockHeight, hash: blockHashB58, timestamp } = streamerMessage.block.header;
    const blockHash = bs58.decode(blockHashB58);
    const keepFromBlockHeight = historyLength && blockHeight - historyLength;
    totalMessages++;
    console.log(new Date(), `Block #${blockHeight} Shards: ${streamerMessage.shards.length}`,
        `Speed: ${totalMessages * 1000 / (Date.now() - timeStarted)} blocks/second`,
        `Lag: ${Date.now() - (timestamp / 1000000)} ms`);

    for (let { stateChanges } of streamerMessage.shards) {
        await redisBatch(async batch => {
            for (let { type, change } of stateChanges) {
                await handleChange({ batch, blockHash, blockHeight, type, change, keepFromBlockHeight });
            }
        });
    }

    await setBlockTimestamp(blockHeight, timestamp);
    await setLatestBlockHeight(blockHeight);
}

async function handleChange({ batch, blockHash, blockHeight, type, change, keepFromBlockHeight }) {
    switch (type) {
        case 'account_update': {
            const { accountId, amount, locked, codeHash, storageUsage } = change;
            const compKey = accountKey(accountId);
            const data = serialize(BORSH_SCHEMA, new Account({ amount, locked, code_hash: bs58.decode(codeHash), storage_usage: storageUsage }));
            await setData(batch)(compKey, blockHash, blockHeight, data);
            if (keepFromBlockHeight) {
                await cleanOlderData(batch)(compKey, keepFromBlockHeight);
            }
            break;
        }
        case 'account_deletion': {
            const { accountId } = change;
            const compKey = accountKey(accountId);
            await deleteData(batch)(compKey, blockHash, blockHeight);
            if (keepFromBlockHeight) {
                await cleanOlderData(batch)(compKey, keepFromBlockHeight);
            }
            break;
        }
        case 'data_update': {
            const { accountId, keyBase64, valueBase64 } = change;
            const storageKey = Buffer.from(keyBase64, 'base64');
            const compKey = dataKey(accountId, storageKey);
            await setData(batch)(compKey, blockHash, blockHeight, Buffer.from(valueBase64, 'base64'));
            if (keepFromBlockHeight) {
                await cleanOlderData(batch)(compKey, keepFromBlockHeight);
            }
            break;
        }
        case 'data_deletion': {
            const { accountId, keyBase64 } = change;
            const storageKey = Buffer.from(keyBase64, 'base64');
            const compKey = dataKey(accountId, storageKey);
            await deleteData(batch)(compKey, blockHash, blockHeight);
            if (keepFromBlockHeight) {
                await cleanOlderData(batch)(compKey, keepFromBlockHeight);
            }
            break;
        }
        case 'contract_code_update': {
            const { accountId, codeBase64 } = change;
            const compKey = codeKey(accountId);
            await setData(batch)(compKey, blockHash, blockHeight, Buffer.from(codeBase64, 'base64'));
            if (keepFromBlockHeight) {
                await cleanOlderData(batch)(compKey, keepFromBlockHeight);
            }
            break;
        }
        case 'contract_code_deletion': {
            const { accountId } = change;
            const compKey = codeKey(accountId);
            await deleteData(batch)(compKey, blockHash, blockHeight);
            if (keepFromBlockHeight) {
                await cleanOlderData(batch)(compKey, keepFromBlockHeight);
            }
            break;
        }
    }
}

const DEFAULT_BATCH_SIZE = 20;

const yargs = require('yargs/yargs');
const storageClient = require("../storage-client");
yargs(process.argv.slice(2))
    .command(['s3 [bucket-name] [start-block-height] [region-name] [endpoint]', '$0'],
            'loads data from NEAR Lake S3 into Redis DB',
            yargs => yargs
                .option('start-block-height', {
                    describe: 'block height to start loading from. By default starts from latest known block height or genesis.',
                    number: true
                })
                .describe('bucket-name', 'S3 bucket name')
                .describe('region-name', 'S3 region name')
                .describe('endpoint', 'S3-compatible storage URL')
                .option('batch-size', {
                    describe: 'how many blocks to try fetch in parallel',
                    number: true,
                    default: DEFAULT_BATCH_SIZE
                })
                .option('history-length', {
                    describe: 'How many latest blocks of history to keep. Unlimited by default.',
                    number: true
                })
                .option('limit', {
                    describe: 'How many blocks to fetch before stopping. Unlimited by default.',
                    number: true
                }),
            async argv => {

        const { startBlockHeight, bucketName, regionName, endpoint, batchSize, historyLength, limit } = argv;
        let blocksProcessed = 0;

        for await (let streamerMessage of stream({
            startBlockHeight: startBlockHeight || await storageClient.getLatestBlockHeight() || 0,
            s3BucketName: bucketName || "near-lake-data-mainnet",
            s3RegionName: regionName || "eu-central-1",
            s3Endpoint: endpoint,
            blocksPreloadPoolSize: batchSize
        })) {
            await withTimeCounter('handleStreamerMessage', async () => {
                await handleStreamerMessage(streamerMessage, { historyLength });
            });

            console.log('counters', getCounters());
            resetCounters();
            blocksProcessed++;
            if (limit && blocksProcessed >= limit) {
                break;
            }
        }

        // TODO: Check what else is blocking exit
        await closeRedis();
    })
    .parse();
