const { stream } = require('near-lake-framework');
const minimatch = require('minimatch');
const bs58 = require('bs58');
const { serialize } = require('borsh');
const storageClient = require("../storage-client");
const { DATA_SCOPE, ACCOUNT_SCOPE, CODE_SCOPE, compositeKey, ACCESS_KEY_SCOPE } = require('../storage-keys');
const { Account, BORSH_SCHEMA, AccessKey, PublicKey, FunctionCallPermission, AccessKeyPermission, FullAccessPermission } = require('../data-model');

const { withTimeCounter, getCounters, resetCounters} = require('../counters');

let totalMessages = 0;
let timeStarted = Date.now();

async function handleStreamerMessage(streamerMessage, { historyLength, include, exclude } = {}) {
    const { height: blockHeight, hash: blockHashB58, timestamp } = streamerMessage.block.header;
    const blockHash = bs58.decode(blockHashB58);
    const keepFromBlockHeight = historyLength && blockHeight - historyLength;
    totalMessages++;
    console.log(new Date(), `Block #${blockHeight} Shards: ${streamerMessage.shards.length}`,
        `Speed: ${totalMessages * 1000 / (Date.now() - timeStarted)} blocks/second`,
        `Lag: ${Date.now() - (timestamp / 1000000)} ms`);

    for (let { stateChanges } of streamerMessage.shards) {
        await storageClient.redisBatch(async batch => {
            for (let { type, change } of stateChanges) {
                await handleChange({ batch, blockHash, blockHeight, type, change, keepFromBlockHeight, include, exclude });
            }
        });
    }

    await storageClient.setBlockTimestamp(blockHeight, timestamp);
    await storageClient.setLatestBlockHeight(blockHeight);
}

async function handleChange({ batch, blockHash, blockHeight, type, change, keepFromBlockHeight, include, exclude }) {
    const handleUpdate = async (scope, accountId, dataKey, data) => {
        await storageClient.setData(batch)(scope, accountId, dataKey, blockHash, blockHeight, data);
        if (keepFromBlockHeight) {
            await storageClient.cleanOlderData(batch)(compositeKey(scope, accountId, dataKey), keepFromBlockHeight);
        }
    }

    const handleDeletion = async (scope, accountId, dataKey) => {
        await storageClient.deleteData(batch)(scope, accountId, dataKey, blockHash, blockHeight);
        if (keepFromBlockHeight) {
            await storageClient.cleanOlderData(batch)(compositeKey(scope, accountId, dataKey), keepFromBlockHeight);
        }
    }

    const { accountId } = change;
    if (include && include.find(pattern => !minimatch(accountId, pattern))) {
        return;
    }
    if (exclude && exclude.find(pattern => minimatch(accountId, pattern))) {
        return;
    }

    switch (type) {
        case 'account_update': {
            const { amount, locked, codeHash, storageUsage } = change;
            await handleUpdate(ACCOUNT_SCOPE, accountId, null,
                serialize(BORSH_SCHEMA, new Account({ amount, locked, code_hash: bs58.decode(codeHash), storage_usage: storageUsage })));
            break;
        }
        case 'account_deletion': {
            // TODO: Check if account_deletion comes together with contract_code_deletion
            await handleDeletion(ACCOUNT_SCOPE, accountId, null);
            break;
        }
        case 'data_update': {
            const { keyBase64, valueBase64 } = change;
            const storageKey = Buffer.from(keyBase64, 'base64');
            await handleUpdate(DATA_SCOPE, accountId, storageKey, Buffer.from(valueBase64, 'base64'));
            break;
        }
        case 'data_deletion': {
            const { keyBase64 } = change;
            const storageKey = Buffer.from(keyBase64, 'base64');
            await handleDeletion(DATA_SCOPE, accountId, storageKey);
            break;
        }
        case 'access_key_update': {
            const { publicKey: publicKeyStr, accessKey: {
                nonce,
                permission 
            } } = change;
            // NOTE: nonce.toString() is a hack to make stuff work, near-lake shouldn't use number for u64 values as it results in data loss
            const accessKey = new AccessKey({ nonce: nonce.toString(), permission: new AccessKeyPermission(
                permission == 'FullAccess'
                    ? { fullAccess: new FullAccessPermission() }
                    : { functionCall: new FunctionCallPermission(permission.FunctionCall) }
            )});
            const storageKey = serialize(BORSH_SCHEMA, PublicKey.fromString(publicKeyStr));
            await handleUpdate(ACCESS_KEY_SCOPE, accountId, storageKey, serialize(BORSH_SCHEMA, accessKey));
            break;
        }
        case 'access_key_deletion': {
            const { publicKey: publicKeyStr } = change;
            const storageKey = serialize(BORSH_SCHEMA, PublicKey.fromString(publicKeyStr));
            await handleDeletion(ACCESS_KEY_SCOPE, accountId, storageKey);
            break;
        }
        case 'contract_code_update': {
            const { codeBase64 } = change;
            await handleUpdate(CODE_SCOPE, accountId, null, Buffer.from(codeBase64, 'base64'));
            break;
        }
        case 'contract_code_deletion': {
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
                    .option('include', {
                        describe: 'include only accounts matching this glob pattern. Can be specified multiple times.',
                        array: true
                    })
                    .option('exclude', {
                        describe: 'exclude accounts matching this glob pattern. Can be specified multiple times.',
                        array: true
                    })
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

            const {
                startBlockHeight,
                bucketName,
                regionName,
                endpoint,
                batchSize,
                historyLength,
                limit,
                include,
                exclude
            } = argv;

            let blocksProcessed = 0;

            for await (let streamerMessage of stream({
                startBlockHeight: startBlockHeight || await storageClient.getLatestBlockHeight() || 0,
                s3BucketName: bucketName || "near-lake-data-mainnet",
                s3RegionName: regionName || "eu-central-1",
                s3Endpoint: endpoint,
                blocksPreloadPoolSize: batchSize
            })) {
                await withTimeCounter('handleStreamerMessage', async () => {
                    await handleStreamerMessage(streamerMessage, { historyLength, include, exclude });
                });

                console.log('counters', getCounters());
                resetCounters();
                blocksProcessed++;
                if (limit && blocksProcessed >= limit) {
                    break;
                }
            }

            // TODO: Check what else is blocking exit
            await storageClient.closeRedis();
        })
        .parse();
}