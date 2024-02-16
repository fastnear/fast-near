
const { mkdir, writeFile, access } = require('fs/promises');

const sha256 = require('../utils/sha256');
const { writeChangesFile, readChangesFile, changeKey, mergeChangesFiles } = require('../storage/lake/changes-index');
const { readBlocks } = require('../storage/lake/archive');

const BLOCKS_PER_BATCH = 10000;

const MIN_CHANGES_PER_FILE = 1000;

async function main() {

    const [, , bucketName, startAfter, limit] = process.argv;

    const startBlockNumber = startAfter ? parseInt(startAfter, 10) : 0;
    const endBlockNumber = startBlockNumber + parseInt(limit, 10);
    const dstDir = `./lake-data/${bucketName}`;
    // TODO: Should index smth from 'block' as well? (e.g. block.header.timestamp)
    const shards = (process.env.FAST_NEAR_SHARDS || '0,1,2,3').split(',');

    for (let shard of shards) {
        console.log('Processing shard', shard);

        const indexDirs = [];
        const standaloneAccounts = new Set();
        for (let start = startBlockNumber; start < endBlockNumber; start += BLOCKS_PER_BATCH) {
            const end = Math.min(start + BLOCKS_PER_BATCH, endBlockNumber);
            console.log('Processing batch', start, end);

            const blocksStream = readBlocks(dstDir, shard, start, end);
            const parseBlocksStream = mapStream(blocksStream, ({ data }) => JSON.parse(data.toString('utf-8')));
            const [stream1, stream2] = ReadableStream.from(parseBlocksStream).tee();

            const allChangesByAccountPromise = reduceStream(
                changesByAccountStream(stream1),
                (a, b) => mergeObjects(a, b, mergeChanges));
            const blobsPromise = extractBlobs(stream2);

            const [allChangesByAccount, ] = await Promise.all([allChangesByAccountPromise, blobsPromise]);
            const indexDir = `${dstDir}/${shard}/index/${start}`;
            indexDirs.push(indexDir);
            Object.keys(allChangesByAccount)
                .filter(accountId => allChangesByAccount[accountId]
                    .reduce((sum, { changes }) => sum + changes.length, 0) > MIN_CHANGES_PER_FILE)
                .forEach(accountId => standaloneAccounts.add(accountId));
            await writeChanges(indexDir, allChangesByAccount);
        }

        // TODO: Filter out standalone accounts from changes.dat
        await mergeChangesFiles(`${dstDir}/${shard}/index/changes.dat`, indexDirs.map(dir => `${dir}/changes.dat`));

        for (let accountId of standaloneAccounts) {
            await mergeChangesFiles(
                `${dstDir}/${shard}/index/${accountId}.dat`,
                await Promise.all(indexDirs.map(async dir =>
                    (await fileExists(`${dir}/${accountId}.dat`)
                    ? `${dir}/${accountId}.dat` : `${dir}/changes.dat`))),
                { accountId });
        }
    }

    async function extractBlobs(blocksStream) {
        const blobDir = `${dstDir}/blob`;
        await mkdir(blobDir, { recursive: true });
        for await (const { state_changes, chunk } of blocksStream) {
            if (!chunk) {
                continue;
            }

            for (let { type, change } of state_changes) {
                if (type === 'contract_code_update') {
                    const { code_base64 } = change;
                    const code = Buffer.from(code_base64, 'base64');
                    const hash = sha256(code).toString('hex');
                    console.log('contract', chunk.header.height_included, change.account_id, hash);
                    const blobPath = `${blobDir}/${hash}.wasm`;
                    await writeFile(blobPath, code);
                }
            }
        }
    }

    async function *changesByAccountStream(blocksStream) {
        for await (const { state_changes, chunk } of blocksStream) {
            if (!chunk) {
                continue;
            }

            const blockHeight = chunk.header.height_included;
            const changesByAccount = {};
            for (let { type, change } of state_changes) {
                // NOTE: No need to index as code hash is in account_update and code is extracted as blobs
                if (type === 'contract_code_update') {
                    continue;
                }

                const { account_id, ...changeData } = change;
                const accountChanges = changesByAccount[account_id];
                const key = changeKey(type, changeData);

                if (!accountChanges) {
                    changesByAccount[account_id] = [{ key, changes: [blockHeight] }];
                } else {
                    const index = accountChanges.findIndex(({ key: k }) => k.equals(key));
                    if (index !== -1) {
                        const changes = accountChanges[index].changes;
                        if (changes.at(-1) !== blockHeight) {
                            changes.push(blockHeight);
                        }
                    } else {
                        accountChanges.push({ key, changes: [blockHeight] });
                    }
                }
            }

            for (let accountChanges of Object.values(changesByAccount)) {
                accountChanges.sort((a, b) => a.key.compare(b.key));
                accountChanges.forEach(({ changes }) => changes.reverse());
            }

            yield changesByAccount;
        }
    }
}

async function fileExists(path) {
    try {
        await access(path);
        return true;
    } catch (error) {
        if (error.code === 'ENOENT') {
            return false;
        }
        throw error;
    }
}

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

async function *mapStream(stream, fn) {
    for await (const item of stream) {
        yield await fn(item);
    }
}

async function consumeStream(stream) {
    for await (const _ of stream) {
        // Do nothing
    }
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
    return mergeSortedArrays(a, b,
        (a, b) => a.key.compare(b.key),
        (a, b) => ({ key: a.key, changes: mergeSortedArrays(a.changes, b.changes, (a, b) => b - a) }));
}

function mergeSortedArrays(a, b, compareFn = (a, b) => a < b ? -1 : a > b ? 1 : 0, mergeFn = (a, b) => a) {
    const result = [];
    let i = 0;
    let j = 0;
    while (i < a.length && j < b.length) {
        const comparison = compareFn(a[i], b[j]);
        if (comparison < 0) {
            result.push(a[i]);
            i++;
        } else if (comparison > 0) {
            result.push(b[j]);
            j++;
        } else {
            result.push(mergeFn(a[i], b[j]));
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