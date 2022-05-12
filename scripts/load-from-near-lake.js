const { startStream } = require("near-lake-framework");
const bs58 = require('bs58');
const { serialize } = require('borsh');
const { setLatestBlockHeight, setData, deleteData } = require('../storage-client');
const { accountKey, dataKey, codeKey } = require('../storage-keys');
const { Account, BORSH_SCHEMA } = require('../data-model');

async function handleStreamerMessage(streamerMessage) {
    const { height: blockHeight, hash: blockHashB58 } = streamerMessage.block.header;
    const blockHash = bs58.decode(blockHashB58);
    console.log(`Block #${blockHeight} Shards: ${streamerMessage.shards.length}`);

    for (let { stateChanges } of streamerMessage.shards) {
        for (let { type, change } of stateChanges) {
            switch (type) {
                case 'account_update': {
                    const { accountId, amount, locked, codeHash, storageUsage } = change;
                    await setData(accountKey(accountId), blockHash, blockHeight,
                        serialize(BORSH_SCHEMA, new Account({ amount, locked, code_hash: bs58.decode(codeHash), storage_usage: storageUsage })))
                    break;
                }
                case 'account_deletion': {
                    const { accountId } = change;
                    await deleteData(accountKey(accountId), blockHash, blockHeight);
                    break;
                }
                case 'data_update': {
                    const { accountId, keyBase64, valueBase64 } = change;
                    const storageKey = Buffer.from(keyBase64, 'base64');
                    await setData(dataKey(accountId, storageKey), blockHash, blockHeight, Buffer.from(valueBase64, 'base64'));
                    break;
                }
                case 'data_deletion': {
                    const { accountId, keyBase64 } = change;
                    const storageKey = Buffer.from(keyBase64, 'base64');
                    await deleteData(dataKey(accountId, storageKey), blockHash, blockHeight);
                    break;
                }
                case 'contract_code_update': {
                    const { accountId, codeBase64 } = change;
                    await setData(codeKey(accountId), blockHash, blockHeight, Buffer.from(codeBase64, 'base64'));
                    break;
                }
                case 'contract_code_deletion': {
                    const { accountId } = change;
                    await deleteData(codeKey(accountId), blockHash, blockHeight);
                    break;
                }
            }
        }
    }

    await setLatestBlockHeight(blockHeight);
}

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
                .describe('endpoint', 'S3-compatible storage URL'),
            async argv => {

        const { startBlockHeight, bucketName, regionName, endpoint } = argv;
        await startStream({
            startBlockHeight: startBlockHeight || await storageClient.getLatestBlockHeight() || 0,
            s3BucketName: bucketName || "near-lake-data-mainnet",
            s3RegionName: regionName || "eu-central-1",
            s3Endpoint: endpoint
        }, handleStreamerMessage);
    })
    .parse();
