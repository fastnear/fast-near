const { stream } = require('near-lake-framework');
const minimatch = require('minimatch');
const bs58 = require('bs58');
const fetch = require('node-fetch');
const FormData = require('form-data');
const { serialize } = require('borsh');
const storageClient = require("../storage-client");
const { DATA_SCOPE, ACCOUNT_SCOPE, CODE_SCOPE, compositeKey, ACCESS_KEY_SCOPE } = require('../storage-keys');
const { Account, BORSH_SCHEMA, AccessKey, PublicKey, FunctionCallPermission, AccessKeyPermission, FullAccessPermission } = require('../data-model');

const { withTimeCounter, getCounters, resetCounters} = require('../counters');

let totalMessages = 0;
let timeStarted = Date.now();

const NUM_RETRIES = 10;
const RETRY_TIMEOUT = 5000;
async function handleStreamerMessage(streamerMessage, options = {}) {
    const { dumpRedis, dumpEstuary, dumpQuestdb } = options;
    const { height: blockHeight, timestamp } = streamerMessage.block.header;
    totalMessages++;
    console.log(new Date(), `Block #${blockHeight} Shards: ${streamerMessage.shards.length}`,
        `Speed: ${totalMessages * 1000 / (Date.now() - timeStarted)} blocks/second`,
        `Lag: ${Date.now() - (timestamp / 1000000)} ms`);
    
    const pipeline = [
        dumpRedis && dumpChangesToRedis,
        dumpEstuary && scheduleUploadToEstuary,
        dumpQuestdb && dumpReceiptsToQuestDB,
    ].filter(Boolean);

    if (pipeline.length === 0) {
        console.warn('NOTE: No data output pipeline configured. Performing dry run.');
    }

    for (let fn of pipeline) {
        await fn(streamerMessage, options);
    }
}

function parseRustEnum(enumObj) {
    if (typeof enumObj === 'string') {
        return [enumObj, {}];
    } else {
        const actionKeys = Object.keys(enumObj);
        if (actionKeys.length !== 1) {
            console.log('rekt enum', enumObj);
            process.exit(1);
        }
        return [actionKeys[0], enumObj[actionKeys[0]]];
    }
}

const { stringify: csvToString } = require('csv-stringify/sync');
const FAST_NEAR_QUESTDB_URL = process.env.FAST_NEAR_QUESTDB_URL || 'http://localhost:9000';

async function dumpReceiptsToQuestDB(streamerMessage) {
    const { height: blockHeight, hash: blockHashB58, timestampNanosec } = streamerMessage.block.header;
    const blockHash = bs58.decode(blockHashB58);
    // TODO: Record IPFS blockhashes?
    const receipts = [];
    for (let shard of streamerMessage.shards) {
        let { chunk } = shard;
        if (!chunk) {
            console.log('rekt block', streamerMessage);
            continue;
        }
        for (let { predecessorId, receipt, receiptId, receiverId } of chunk.receipts) {
            if (receipt.Action) {
                let index_in_action_receipt = 0;
                for (let action of receipt.Action.actions) {
                    const [action_kind, actionArgs] = parseRustEnum(action);
                    let receiptData = {
                        ts: new Date(Number(BigInt(timestampNanosec) / BigInt(1000000))).toISOString(),
                        receipt_id: receiptId,
                        index_in_action_receipt,
                        action_kind,
                        deposit: actionArgs.deposit,
                        method_name: actionArgs.methodName,
                        args_base64: actionArgs.args,
                        receiver_id: receiverId,
                        predecessor_id: predecessorId,
                        // transaction_hash: null, // TODO: Join with transactions?
                        signer_id: receipt.Action.signerId,
                        signer_public_key: receipt.Action.signerPublicKey,
                    }
                    index_in_action_receipt++;
                    receipts.push(receiptData);
                }
            } else {
                console.log('Skipping receipt', receipt);
            }
        }
    }
    if (receipts.length > 0) {
        const csv = csvToString(receipts, { header: true });
        const formData = new FormData();
        formData.append('schema', JSON.stringify([
            { name: "action_kind", type: "SYMBOL" },
            { name: "receiver_id", type: "SYMBOL" },
            { name: "predecessor_id", type: "SYMBOL" },
            { name: "signer_id", type: "SYMBOL" },
            { name: "method_name", type: "SYMBOL" },
        ]), 'schema');
        formData.append('data', csv, 'data');
        console.log('importing', receipts.length, 'receipts for block', blockHeight);
        const res = await fetch(`${FAST_NEAR_QUESTDB_URL}/imp?fmt=json&forceHeader=true`, {
            method: 'POST',
            body: formData,
        });
        if (!res.ok) {
            console.log('res', res.status, await res.text());
            process.exit(1);
        }
        console.log('imported receipts for block', blockHeight);
    }
}

async function dumpChangesToRedis(streamerMessage, { historyLength, include, exclude } = {}) {
    // TODO: Use timestampNanoSec?
    const { height: blockHeight, hash: blockHashB58, timestamp } = streamerMessage.block.header;
    const blockHash = bs58.decode(blockHashB58);
    const keepFromBlockHeight = historyLength && blockHeight - historyLength;

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

const uploadQueue = [];

async function scheduleUploadToEstuary(streamerMessage, { batchSize }) {
    const { height: blockHeight, hash: blockHashB58 } = streamerMessage.block.header;

    if (uploadQueue.length >= batchSize) {
        await Promise.race(uploadQueue);
    }

    const upload = async () => {
        const ESTUARY_TOKEN = process.env.ESTUARY_TOKEN;

        const streamerMessageData = Buffer.from(JSON.stringify(streamerMessage));

        const zlib = require('zlib');
        const gzip = zlib.createGzip();
        const compressed = await new Promise((resolve, reject) => {
            const chunks = [];
            gzip.on('data', (chunk) => chunks.push(chunk));
            gzip.on('end', () => resolve(Buffer.concat(chunks)));
            gzip.on('error', reject);
            gzip.write(streamerMessageData);
            gzip.end();
        });
        

        for (let i = 0; i < NUM_RETRIES; i++) {
            const formData = new FormData();
            formData.append('data', compressed, `${blockHashB58}.json.gz`);
            const res = await fetch('https://upload.estuary.tech/content/add', {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${ESTUARY_TOKEN}`,
                },
                body: formData
            });

            if (!res.ok) {
                console.log('Error uploading to Estuary:', res.status, await res.text());
                await new Promise(resolve => setTimeout(resolve, RETRY_TIMEOUT));
            } else {
                const { cid } = await res.json();
                console.log(blockHeight, cid);
                return;
            }
        }
        throw new Error('Too many retries');
    }

    const promise = upload();
    uploadQueue.push(promise);
    promise
        .catch(e => {
            console.log('Error uploading', e);
            process.exit(1); })
        .then(() => uploadQueue.splice(uploadQueue.indexOf(promise), 1));
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
    handleStreamerMessage,
    dumpChangesToRedis,
    dumpReceiptsToQuestDB,
    scheduleUploadToEstuary,
}

if (require.main === module) {
    const DEFAULT_BATCH_SIZE = 20;

    const yargs = require('yargs/yargs');
    yargs(process.argv.slice(2))
        .command(['s3 [bucket-name] [start-block-height] [region-name] [endpoint]', '$0'],
                'loads data from NEAR Lake S3 into other datastores',
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
                    })
                    .option('dump-redis', {
                        describe: 'Dump state changes into Redis. FAST_NEAR_REDIS_URL environment variable to be set to choose Redis host.',
                        boolean: true
                    })
                    .option('dump-estuary', {
                        describe: 'Dump blocks into IPFS using Estuary. Requires ESTUARY_TOKEN environment variable to be set to auth token. See https://docs.estuary.tech/tutorial-get-an-api-key for more information.',
                        boolean: true
                    })
                    .option('dump-questdb', {
                        describe: 'Dump receipts into QuestDB. Requires FAST_NEAR_QUESTDB_URL environment variable to be set to QuestDB host. Defaults to http://localhost:9000.',
                        boolean: true
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
                exclude,
                dumpRedis,
                dumpEstuary,
                dumpQuestdb,
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
                    await handleStreamerMessage(streamerMessage, {
                        batchSize,
                        historyLength,
                        include,
                        exclude,
                        dumpRedis,
                        dumpEstuary,
                        dumpQuestdb,
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
            await storageClient.closeRedis();
        })
        .parse();
}