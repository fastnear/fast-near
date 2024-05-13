const minimatch = require('minimatch');
const bs58 = require('bs58');
const { serialize } = require('borsh');
const storage = require("../storage");
const { DATA_SCOPE, ACCOUNT_SCOPE, compositeKey, ACCESS_KEY_SCOPE } = require('../storage-keys');
const { Account, BORSH_SCHEMA, AccessKey, PublicKey, FunctionCallPermission, AccessKeyPermission, FullAccessPermission } = require('../data-model');


const { withTimeCounter, getCounters, resetCounters} = require('../utils/counters');


let totalMessages = 0;
let timeStarted = Date.now();

function formatDuration(milliseconds) {
    let seconds = Math.floor((milliseconds / 1000) % 60);
    let minutes = Math.floor((milliseconds / (1000 * 60)) % 60);
    let hours = Math.floor((milliseconds / (1000 * 60 * 60)) % 24);
    let days = Math.floor((milliseconds / (1000 * 60 * 60 * 24)));
    return [days, hours, minutes, seconds].map(n => n.toString().padStart(2, '0')).join(':');
}

const NUM_RETRIES = 10;
const RETRY_TIMEOUT = 5000;
async function handleStreamerMessage(streamerMessage, options = {}) {
    const { dumpChanges } = options;
    const { height: blockHeight, timestamp } = streamerMessage.block.header;
    totalMessages++;
    console.log(new Date(), `Block #${blockHeight} Shards: ${streamerMessage.shards.length}`,
        `Speed: ${totalMessages * 1000 / (Date.now() - timeStarted)} blocks/second`,
        `Lag: ${formatDuration(Date.now() - (timestamp / 1000000))}`);

    const pipeline = [
        dumpChanges && dumpChangesToStorage,
    ].filter(Boolean);

    if (pipeline.length === 0) {
        console.warn('NOTE: No data output pipeline configured. Performing dry run.');
    }

    for (let fn of pipeline) {
        await fn(streamerMessage, options);
    }
}

async function dumpChangesToStorage(streamerMessage, { historyLength, updateBlockHeight = true, include, exclude } = {}) {
    // TODO: Use timestampNanoSec?
    const { height: blockHeight, hash: blockHashB58, timestamp } = streamerMessage.block.header;
    const blockHash = bs58.decode(blockHashB58);
    const keepFromBlockHeight = historyLength && blockHeight - historyLength;

    console.time('dumpChangesToStorage');
    await storage.writeBatch(async batch => {
        for (let { state_changes } of streamerMessage.shards) {
            for (let { type, change } of state_changes) {
                await handleChange({ batch, blockHash, blockHeight, type, change, keepFromBlockHeight, include, exclude });
            }
        }
    });

    await storage.setBlockTimestamp(blockHeight, timestamp);
    if (updateBlockHeight) {
        await storage.setLatestBlockHeight(blockHeight);
    }
    console.timeEnd('dumpChangesToStorage');
    // TODO: Record block hash to block height mapping?
}

async function handleChange({ batch, blockHeight, type, change, keepFromBlockHeight, include, exclude }) {
    const handleUpdate = async (scope, accountId, dataKey, data) => {
        await storage.setData(batch, scope, accountId, dataKey, blockHeight, data);
        if (keepFromBlockHeight) {
            await storage.cleanOlderData(batch, compositeKey(scope, accountId, dataKey), keepFromBlockHeight);
        }
    }

    const handleDeletion = async (scope, accountId, dataKey) => {
        await storage.deleteData(batch, scope, accountId, dataKey, blockHeight);
        if (keepFromBlockHeight) {
            await storage.cleanOlderData(batch, compositeKey(scope, accountId, dataKey), keepFromBlockHeight);
        }
    }

    const { account_id } = change;
    if (include && include.find(pattern => !minimatch(account_id, pattern))) {
        return;
    }
    if (exclude && exclude.find(pattern => minimatch(account_id, pattern))) {
        return;
    }

    switch (type) {
        case 'account_update': {
            const { amount, locked, code_hash, storage_usage } = change;
            await handleUpdate(ACCOUNT_SCOPE, account_id, null,
                serialize(BORSH_SCHEMA, new Account({ amount, locked, code_hash: bs58.decode(code_hash), storage_usage })));
            break;
        }
        case 'account_deletion': {
            // TODO: Check if account_deletion comes together with contract_code_deletion
            await handleDeletion(ACCOUNT_SCOPE, account_id, null);
            break;
        }
        case 'data_update': {
            const { key_base64, value_base64 } = change;
            const storageKey = Buffer.from(key_base64, 'base64');
            await handleUpdate(DATA_SCOPE, account_id, storageKey, Buffer.from(value_base64, 'base64'));
            break;
        }
        case 'data_deletion': {
            const { key_base64 } = change;
            const storageKey = Buffer.from(key_base64, 'base64');
            await handleDeletion(DATA_SCOPE, account_id, storageKey);
            break;
        }
        case 'access_key_update': {
            const { public_key: publicKeyStr, access_key: {
                nonce,
                permission
            } } = change;
            // NOTE: nonce.toString() is a hack to make stuff work, near-lake shouldn't use number for u64 values as it results in data loss
            const accessKey = new AccessKey({ nonce: nonce.toString(), permission: new AccessKeyPermission(
                permission == 'FullAccess'
                    ? { fullAccess: new FullAccessPermission() }
                    : { functionCall: new FunctionCallPermission({
                        // TODO: Normalize field names in data-model
                        receiverId: permission.FunctionCall.receiver_id,
                        methodNames: permission.FunctionCall.method_names,
                        allowance: permission.FunctionCall.allowance,
                    }) }
            )});
            const storageKey = serialize(BORSH_SCHEMA, PublicKey.fromString(publicKeyStr));
            await handleUpdate(ACCESS_KEY_SCOPE, account_id, storageKey, serialize(BORSH_SCHEMA, accessKey));
            break;
        }
        case 'access_key_deletion': {
            const { public_key: publicKeyStr } = change;
            const storageKey = serialize(BORSH_SCHEMA, PublicKey.fromString(publicKeyStr));
            await handleDeletion(ACCESS_KEY_SCOPE, account_id, storageKey);
            break;
        }
        case 'contract_code_update': {
            const { code_base64 } = change;
            await storage.setBlob(batch, Buffer.from(code_base64, 'base64'));
            break;
        }
        case 'contract_code_deletion': {
            // TODO: Garbage collect unreferenced contract code? Should it happen in corresponding account_update?
            break;
        }
    }
}

module.exports = {
    handleStreamerMessage,
    dumpChangesToStorage,
}

if (require.main === module) {
    const DEFAULT_BATCH_SIZE = 20;

    const yargs = require('yargs/yargs');
    yargs(process.argv.slice(2))
        .command(['load-from-source', '$0'],
                'loads data from given source into fast-near compatible storage',
                yargs => yargs
                    .option('start-block-height', {
                        describe: 'block height to start loading from. By default starts from latest known block height or genesis.',
                        number: true
                    })
                    .option('redis-url', {
                        describe: 'URL of the Redis server to stream data from',
                        // TODO: Require only when source specified
                        // required: true
                    })
                    .option('stream-key', {
                        describe: 'Redis stream key to stream data from',
                        default: 'final_blocks',
                    })
                    .option('data-dir', {
                        describe: 'Directory use as source of lake data. Defaults to `./lake-data/mainnet`.',
                        default: './lake-data/mainnet',
                    })
                    .option('bucket', {
                        describe: 'S3 bucket name',
                        default: 'near-lake-data-mainnet',
                    })
                    .option('region', {
                        describe: 'S3 region name',
                        default: 'eu-central-1',
                    })
                    .describe('endpoint', 'S3-compatible storage URL')
                    .option('shards', {
                        describe: 'Shards to process. Defaults to 0..3',
                        default: ['0', '1', '2', '3'],
                        array: true,
                    })
                    .option('update-block-height', {
                        describe: 'update block height in storage',
                        boolean: true,
                        default: true
                    })
                    .option('include', {
                        describe: 'include only accounts matching this glob pattern. Can be specified multiple times.',
                        array: true
                    })
                    .option('exclude', {
                        describe: 'exclude accounts matching this glob pattern. Can be specified multiple times.',
                        array: true
                    })
                    // TODO: Check if batch size still relevant
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
                    })
                    .option('dump-changes', {
                        describe: 'Dump state changes into storage. Use FAST_NEAR_STORAGE_TYPE to specify storage type. Defaults to `redis`.',
                        boolean: true
                    })
                    .option('update-block-height', {
                        describe: 'update block height in storage',
                        boolean: true,
                        default: true
                    })
                    .option('source', {
                        describe: 'Source of the data. Defaults to `redis-blocks`.',
                        choices: ['redis-blocks', 'lake', 's3-lake', 'neardata'],
                        default: 'redis-blocks'
                    }),
                async argv => {

            const {
                startBlockHeight,
                batchSize,
                historyLength,
                limit,
                updateBlockHeight,
                include,
                exclude,
                dumpChanges,
                source,
                ...otherOptions
            } = argv;
            console.log('otherOptions', otherOptions);

            let blocksProcessed = 0;

            const { readBlocks } = require(`../source/${source}`);

            const start = startBlockHeight || await storage.getLatestBlockHeight() || 0;
            for await (let streamerMessage of readBlocks({
                startBlockHeight: start,
                endBlockHeight: limit ? start + limit : undefined,
                batchSize,
                ...otherOptions
            })) {
                await withTimeCounter('handleStreamerMessage', async () => {
                    await handleStreamerMessage(streamerMessage, {
                        batchSize,
                        historyLength,
                        updateBlockHeight,
                        include,
                        exclude,
                        dumpChanges,
                    });
                });

                // console.log('counters', getCounters());
                resetCounters();
                blocksProcessed++;
                if (limit && blocksProcessed >= limit) {
                    break;
                }
            }

            // TODO: Check what else is blocking exit
            await storage.closeDatabase();
        })
        .parse();
}