
const { mkdir } = require('fs/promises');

const { writeChangesFile, readChangesFile, changeKey } = require('../storage/lake/changes-index');
const { readBlocks } = require('../storage/lake/archive');

async function main() {

    const [, , bucketName, startAfter, limit] = process.argv;

    const startBlockNumber = startAfter ? parseInt(startAfter, 10) : 0;
    const endBlockNumber = startBlockNumber + parseInt(limit, 10);
    const dstDir = `./lake-data/${bucketName}`;
    // TODO: Make shards dynamic, allow to filter by shard
    // TODO: Should index smth from 'block' as well? (e.g. block.header.timestamp)
    // const shards = ['0', '1', '2', '3'];
    const shards = ['0'];

    for (let shard of shards) {
        console.log('Processing shard', shard);
        const allChangesByAccount = await reduceStream(
            changesByAccountStream(shard, startBlockNumber, endBlockNumber),
            (a, b) => mergeObjects(a, b, mergeChanges));
        await writeChanges(`${dstDir}/${shard}/index`, allChangesByAccount);
    }

    async function *changesByAccountStream(shard, startBlockNumber, endBlockNumber) {
        const blocksStream = readBlocks(dstDir, shard, startBlockNumber, endBlockNumber);
        for await (const { data } of blocksStream) {
            const { state_changes, chunk } = JSON.parse(data.toString('utf-8'));
            if (!chunk) {
                continue;
            }

            const blockHeight = chunk.header.height_included;
            const changesByAccount = {};
            for (let { type, change } of state_changes) {
                const { account_id, ...changeData } = change;
                const accountChanges = changesByAccount[account_id];
                const key = changeKey(type, changeData);

                if (!accountChanges) {
                    changesByAccount[account_id] = [{ key, changes: [blockHeight] }];
                } else {
                    const index = accountChanges.findIndex(({ key: k }) => k.equals(key));
                    if (index !== -1) {
                        accountChanges[index].changes.push(blockHeight);
                    } else {
                        accountChanges.push({ key, changes: [blockHeight] });
                    }
                }
            }

            for (let accountChanges of Object.values(changesByAccount)) {
                accountChanges.sort((a, b) => a.key.compare(b.key));
            }

            yield changesByAccount;
        }
    }
}

const MIN_CHANGES_PER_FILE = 1000;

async function writeChanges(outFolder, changesByAccount) {
    await mkdir(outFolder, { recursive: true });

    for (let accountId in changesByAccount) {
        const accountChanges = changesByAccount[accountId];
        const totalChanges = accountChanges.reduce((sum, { changes }) => sum + changes.length, 0);
        if (totalChanges < MIN_CHANGES_PER_FILE) {
            continue;
        }

        await writeChangesFile(`${outFolder}/${accountId}.dat`, { [accountId]: accountChanges });
        delete changesByAccount[accountId];
    }

    await writeChangesFile(`${outFolder}/changes.dat`, changesByAccount);

    // TODO: Remove this debuggin code
    // for await (const { accountId, key, changes } of readChangesFile(`${outFolder}/changes.dat`, {
    //         // accountId: 'atocha.octopus-registry.near',
    //         // keyPrefix: Buffer.from('64', 'hex'),
    //         accountId: '4eeeda737da24f64610731c8b140174a981b8efebcf4437d8e6ad9fb0ad8abdd',
    //         keyPrefix: Buffer.from('6b004eee', 'hex'),
    //     })) {
    //     console.log('readChangesFile:', accountId, key, changes);
    // }

    for await (const { accountId, key, changes } of readChangesFile(`${outFolder}/app.nearcrowd.near.dat`, {
            accountId: 'app.nearcrowd.near',
            keyPrefix: Buffer.from('a'),
            // keyPrefix: Buffer.from('6b00', 'hex'),
            // keyPrefix: Buffer.from('64', 'hex'),
            // keyPrefix: Buffer.from('6474', 'hex'),
        })) {
        console.log('readChangesFile:', accountId, key, changes);
    }
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

async function reduceStream(stream, fn) {
    // TODO: Adjust / pass as option?
    const MAX_CHUNK_SIZE = 8 * 1024;
    let chunkSize = 16
    let processed = 0;

    const chunk = [];
    let result;
    for await (const item of stream) {
        chunk.push(item);

        if (chunk.length >= chunkSize) {
            if (result === undefined) {
                result = reduceRecursive(chunk, fn);
            } else {
                result = fn(result, reduceRecursive(chunk, fn));
            }
            processed += chunk.length;
            chunkSize = Math.min(MAX_CHUNK_SIZE, processed);
            chunk.length = 0;
        }
    }

    if (chunk.length === 0) {
        return result;
    }

    if (result === undefined) {
        return reduceRecursive(chunk, fn);
    }

    return fn(result, reduceRecursive(chunk, fn));
}

function mergeObjects(a, b, fn) {
    for (k in b) {
        if (a[k]) {
            a[k] = fn(a[k], b[k]);
        } else {
            a[k] = b[k];
        }
    }
    return a;
}

function mergeChanges(a, b) {
    return mergeSortedArrays(a, b, (a, b) => a.key.compare(b.key));
}

function mergeSortedArrays(a, b, fn = (a, b) => a < b ? -1 : a > b ? 1 : 0) {
    const result = [];
    let i = 0;
    let j = 0;
    while (i < a.length && j < b.length) {
        const comparison = fn(a[i], b[j]);
        if (comparison < 0) {
            result.push(a[i]);
            i++;
        } else if (comparison > 0) {
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

if (process.argv.length < 3) {
    console.error('Usage: node scripts/build-raw-near-lake-index <bucketName> [startAfter] [limit]');
    process.exit(1);
}

main().catch((error) => {
    console.error('Exiting because of error', error);
    process.exit(1);
});