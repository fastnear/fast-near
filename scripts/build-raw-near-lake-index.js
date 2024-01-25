
const compressing = require('compressing');

const fs = require('fs');
const zlib = require('zlib');
const tar = require('tar-stream');

async function main() {

    const [, , bucketName, startAfter, limit] = process.argv;

    const FILES_PER_ARCHIVE = 5;

    const startBlockNumber = startAfter ? Math.floor(parseInt(startAfter, 10) / FILES_PER_ARCHIVE) * FILES_PER_ARCHIVE : 0;
    const endBlockNumber = startBlockNumber + parseInt(limit, 10);
    const dstDir = `./lake-data/${bucketName}`;
    // TODO: Make shards dynamic, allow to filter by shard
    // TODO: Should index smth from 'block' as well? (e.g. block.header.timestamp)
    const folders = ['0', '1', '2', '3'];

    for (let folder of folders) {
        console.log('Processing shard', folder);

        for (let blockNumber = startBlockNumber; blockNumber < endBlockNumber; blockNumber += FILES_PER_ARCHIVE) {
            console.log('blockNumber', blockNumber, 'endBlockNumber', endBlockNumber);
            const blockHeight = normalizeBlockHeight(blockNumber);
            const [prefix1, prefix2] = blockHeight.match(/^(.{6})(.{3})/).slice(1);
            const inFolder = `${dstDir}/${folder}/${prefix1}/${prefix2}`;
            const inFile = `${inFolder}/${blockHeight}.tgz`;
            // const uncompressStream = new compressing.tgz.UncompressStream({ source: inFile });

            console.log('inFile', inFile);

            const extract = tar.extract();

            const gunzip = zlib.createGunzip();
            const readStream = fs.createReadStream(inFile);
            readStream.pipe(gunzip).pipe(extract);

            const changesByAccount = {};
            for await (const entry of extract) {
                // console.log('header', entry.header);

                // Convert entry stream into data buffer
                const data = await new Promise((resolve, reject) => {
                    const chunks = [];
                    entry.on('data', (chunk) => chunks.push(chunk));
                    entry.on('end', () => resolve(Buffer.concat(chunks)));
                    entry.on('error', reject);
                });

                const { state_changes, chunk,  ...json } = JSON.parse(data.toString('utf-8'));
                const blockHeight = chunk.header.height_included;
                console.log('blockHeight', blockHeight);
                // console.log('json', json);
                // console.log('state_changes', state_changes);

                for (let { type, change } of state_changes) {
                    const { account_id, ...changeData } = change;
                    const accountChanges = changesByAccount[account_id];
                    const key = changeKey(type, changeData);
                    if (!accountChanges) {
                        changesByAccount[account_id] = { [key]: [blockHeight] };
                    } else {
                        if (accountChanges[key] && accountChanges[key].at(-1) != blockHeight) {
                            accountChanges[key].push(blockHeight);
                        } else {
                            accountChanges[key] = [blockHeight];
                        }
                    }
                }
            }

            console.log('changesByAccount', changesByAccount);
        }
    }
    
}

function changeKey(type, changeData) {
    // TODO: Adjust this as needed
    switch (type) {
        case 'account_update':
        case 'account_deletion':
            return type;
        case 'access_key_update':
        case 'access_key_deletion':
            return `${type}:${changeData.public_key}`;
        case 'data_update':
        case 'data_deletion':
            return `${type}:${changeData.key_base64}`;
        case 'contract_code_update':
        case 'contract_code_deletion':
            return `${type}`;
        default:
            throw new Error(`Unknown type ${type}`);
    }
}

function normalizeBlockHeight(number) {
    return number.toString().padStart(12, '0');
}

if (process.argv.length < 3) {
    console.error('Usage: node scripts/build-raw-near-lake-index <bucketName> [startAfter] [limit]');
    process.exit(1);
}

main().catch((error) => {
    console.error('Exiting because of error', error);
    process.exit(1);
});