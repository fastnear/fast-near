
const fs = require('fs');
const { writeFile } = require('fs/promises');
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
    const shards = ['0', '1', '2', '3'];

    for (let shard of shards) {
        console.log('Processing shard', shard);

        const unmerged = [];
        for await (const changesByAccount of changesByAccountStream(shard, startBlockNumber, endBlockNumber)) {
            unmerged.push(changesByAccount);
        }

        const allChangesByAccount = mergeChangesByAccount(unmerged);
        // console.log('allChangesByAccount', allChangesByAccount);

        await writeChanges(`${dstDir}/${shard}`, allChangesByAccount);
    }

    async function *changesByAccountStream(shard, startBlockNumber, endBlockNumber) {
        for (let blockNumber = startBlockNumber; blockNumber < endBlockNumber; blockNumber += FILES_PER_ARCHIVE) {
            console.log('blockNumber', blockNumber, 'endBlockNumber', endBlockNumber);
            const blockHeight = normalizeBlockHeight(blockNumber);
            const [prefix1, prefix2] = blockHeight.match(/^(.{6})(.{3})/).slice(1);
            const inFolder = `${dstDir}/${shard}/${prefix1}/${prefix2}`;
            const inFile = `${inFolder}/${blockHeight}.tgz`;

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
                if (!chunk) {
                    continue;
                }

                const blockHeight = chunk.header.height_included;

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

            yield changesByAccount;
        }
    }
}

const MIN_CHANGES_PER_FILE = 1000;

async function writeChanges(outFolder, changesByAccount) {
    console.log('changesByAccount', changesByAccount);
    for (let accountId in changesByAccount) {
        const accountChanges = changesByAccount[accountId];
        const totalChanges = Object.values(accountChanges).reduce((sum, changes) => sum + changes.length, 0);
        if (totalChanges < MIN_CHANGES_PER_FILE) {
            continue;
        }

        await writeChangesFile(`${outFolder}/${accountId}.dat`, { [accountId]: accountChanges });
        delete changesByAccount[accountId];
    }


    await writeChangesFile(`${outFolder}/changes.dat`, changesByAccount);
}

const PAGE_SIZE = 64 * 1024;

async function writeChangesFile(outPath, changesByAccount) {
    console.log('writeChangesFile', outPath, Object.keys(changesByAccount).length);

    const outStream = fs.createWriteStream(outPath);
    const buffer = Buffer.alloc(PAGE_SIZE);
    let offset = 0;
    for (let accountId in changesByAccount) {
        offset = buffer.writeUInt8(accountId.length, offset);
        offset += buffer.write(accountId, offset);

        const accountChanges = changesByAccount[accountId];

        for (let key in accountChanges) {
            const allChanges = accountChanges[key];

            // NOTE: Changes arrays are split into chunks of 0xFF items
            for (let i = 0; i < allChanges.length + 0xFF; i += 0xFF) {
                const changes = allChanges.slice(i, i + 0xFF);

                // TODO: Check max key length
                offset = buffer.writeUInt8(key.length, offset);
                offset += buffer.write(key, offset);

                offset = buffer.writeUInt8(changes.length, offset);
                for (let change of changes) {
                    offset = buffer.writeInt32LE(change, offset);

                    // TODO: Adjust this as needed
                    if (offset > PAGE_SIZE - 1000) {
                        console.log('Writing', outPath, offset);
                        await new Promise((resolve, reject) => {
                            outStream.write(buffer.slice(0, offset), e => e ? reject(e) : resolve());
                        });
                        offset = 0;
                    }
                }
            }
        }
    }

    await new Promise((resolve, reject) => {
        console.log('Writing', outPath, offset);
        outStream.end(buffer.slice(0, offset), e => e ? reject(e) : resolve());
    });
}

function reduceRecursive(items, fn) {
    if (items.length === 0) {
        throw new Error('Cannot reduce empty list');
    }

    if (items.length === 1) {
        return items[0];
    }

    return fn(
        reduceRecursive(items.slice(0, items.length / 2), fn),
        reduceRecursive(items.slice(items.length / 2), fn));
}

function merge(a, b, fn) {
    for (k in b) {
        if (a[k]) {
            a[k] = fn(a[k], b[k]);
        } else {
            a[k] = b[k];
        }
    }
    return a;
}

function mergeChangesByAccount(changesByAccountList) {
    return reduceRecursive(changesByAccountList, (a, b) => merge(a, b, mergeChanges));
}

function mergeChanges(a, b) {
    return merge(a, b, mergeSortedArrays);
}

function mergeSortedArrays(a, b) {
    const result = [];
    let i = 0;
    let j = 0;
    while (i < a.length && j < b.length) {
        if (a[i] < b[j]) {
            result.push(a[i]);
            i++;
        } else if (a[i] > b[j]) {
            result.push(b[j]);
            j++;
        } else {
            result.push(a[i]);
            i++;
            j++;
        }
    }

    for (; i < a.length; i++) {
        result.push(a[i]);
    }

    for (; j < b.length; j++) {
        result.push(b[j]);
    }

    return result;
}

function changeKey(type, changeData) {
    // TODO: Adjust this as needed
    switch (type) {
        case 'account_update':
        case 'account_deletion':
            return 'a:';
        case 'access_key_update':
        case 'access_key_deletion':
            return `k:${changeData.public_key}`;
        case 'data_update':
        case 'data_deletion':
            return `d:${changeData.key_base64}`;
        case 'contract_code_update':
        case 'contract_code_deletion':
            return 'c:';
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