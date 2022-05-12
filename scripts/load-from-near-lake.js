const { startStream } = require("near-lake-framework");
const bs58 = require('bs58');
const { serialize } = require('borsh');
const { setLatestBlockHeight, setAccountData, setData, deleteData } = require('../storage-client');
const { Account, BORSH_SCHEMA } = require('../data-model');

async function handleStreamerMessage(streamerMessage) {
    const { height: blockHeight, hash: blockHashB58 } = streamerMessage.block.header;
    const blockHash = bs58.decode(blockHashB58);
    console.log(`Block #${blockHeight} Shards: ${streamerMessage.shards.length}`);
    console.log('streamerMessage', streamerMessage);

    for (let { stateChanges } of streamerMessage.shards) {
        for (let { type, change } of stateChanges) {
            console.log(type, change);
            switch (type) {
                case 'account_update': {
                    const { accountId, amount, locked, codeHash, storageUsage } = change;
                    await setAccountData(accountId, blockHash, blockHeight,
                        serialize(BORSH_SCHEMA, new Account({ amount, locked, code_hash: bs58.decode(codeHash), storage_usage: storageUsage })))
                    break;
                }
                case 'data_update': {
                    const { accountId, keyBase64, valueBase64 } = change;
                    const storageKey = Buffer.from(keyBase64, 'base64');
                    // TODO: Refactor compKey logic?
                    const compKey = Buffer.concat([Buffer.from(`${accountId}:`), storageKey]);
                    await setData(compKey, blockHash, blockHeight,
                        Buffer.from(valueBase64, 'base64'));
                    break;
                }
                case 'data_deletion': {
                    const { accountId, keyBase64 } = change;
                    const storageKey = Buffer.from(keyBase64, 'base64');
                    const compKey = Buffer.concat([Buffer.from(`${accountId}:`), storageKey]);
                    await deleteData(compKey, blockHash, blockHeigh);
                    break;
                }
            }
        }
    }

    await setLatestBlockHeight(streamerMessage);
}

const yargs = require('yargs/yargs')
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
            // TODO: Read latest block height from Redis
            startBlockHeight: startBlockHeight || 0,
            s3BucketName: bucketName || "near-lake-data-mainnet",
            s3RegionName: regionName || "eu-central-1",
            s3Endpoint: endpoint

        }, handleStreamerMessage);
    })
    .parse();
