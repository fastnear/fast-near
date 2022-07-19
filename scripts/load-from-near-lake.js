const { stream } = require('near-lake-framework');
const bs58 = require('bs58');
const { serialize } = require('borsh');
const { setLatestBlockHeight, setData, deleteData, cleanOlderData, redisBatch, closeRedis, setBlockTimestamp } = require('../storage-client');
const { accountKey, dataKey, codeKey, DATA_SCOPE, ACCOUNT_SCOPE, CODE_SCOPE, compositeKey } = require('../storage-keys');
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
    const handleUpdate = async (scope, accountId, dataKey, data) => {
        await setData(batch)(scope, accountId, dataKey, blockHash, blockHeight, data);
        if (keepFromBlockHeight) {
            await cleanOlderData(batch)(compositeKey(scope, accountId, dataKey), keepFromBlockHeight);
        }
    }

    const handleDeletion = async (scope, accountId, dataKey) => {
        await deleteData(batch)(scope, accountId, dataKey, blockHash, blockHeight);
        if (keepFromBlockHeight) {
            await cleanOlderData(batch)(compositeKey(scope, accountId, dataKey), keepFromBlockHeight);
        }
    }

    switch (type) {
        case 'account_update': {
            const { accountId, amount, locked, codeHash, storageUsage } = change;
            await handleUpdate(ACCOUNT_SCOPE, accountId, null,
                serialize(BORSH_SCHEMA, new Account({ amount, locked, code_hash: bs58.decode(codeHash), storage_usage: storageUsage })));
            break;
        }
        case 'account_deletion': {
            // TODO: Check if account_deletion comes together with contract_code_deletion
            const { accountId } = change;
            await handleDeletion(ACCOUNT_SCOPE, accountId, null);
            break;
        }
        case 'data_update': {
            const { accountId, keyBase64, valueBase64 } = change;
            const storageKey = Buffer.from(keyBase64, 'base64');
            await handleUpdate(DATA_SCOPE, accountId, storageKey, Buffer.from(valueBase64, 'base64'));
            break;
        }
        case 'data_deletion': {
            const { accountId, keyBase64 } = change;
            const storageKey = Buffer.from(keyBase64, 'base64');
            await handleDeletion(DATA_SCOPE, accountId, storageKey);
            break;
        }
        case 'contract_code_update': {
            const { accountId, codeBase64 } = change;
            await handleUpdate(CODE_SCOPE, accountId, null, Buffer.from(codeBase64, 'base64'));
            break;
        }
        case 'contract_code_deletion': {
            const { accountId } = change;
            await handleDeletion(CODE_SCOPE, accountId, null);
            break;
        }
    }
}

module.exports = {
    handleStreamerMessage
}

if (require.main === module) {
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
}