
const fs = require('fs');
const { writeFile, open, mkdir } = require('fs/promises');
const zlib = require('zlib');
const tar = require('tar-stream');
const bs58 = require('bs58');

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
        const allChangesByAccount = await reduceStream(
            changesByAccountStream(shard, startBlockNumber, endBlockNumber),
            (a, b) => mergeObjects(a, b, mergeChanges));
        await writeChanges(`${dstDir}/${shard}/index`, allChangesByAccount);
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
                        changesByAccount[account_id] = [ { key, changes: [blockHeight] } ];
                    } else {
                        const index = accountChanges.findIndex(({ key: k }) => k.equals(key));
                        if (index !== -1) {
                            accountChanges[index].changes.push(blockHeight);
                        } else {
                            accountChanges.push({ key, changes: [blockHeight] });
                        }
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
    for await (const { accountId, key, changes } of readChangesFile(`${outFolder}/changes.dat`)) {
        console.log('readChangesFile:', accountId, key, changes);
    }
}

const PAGE_SIZE = 64 * 1024;

async function writeChangesFile(outPath, changesByAccount) {
    console.log('writeChangesFile', outPath, Object.keys(changesByAccount).length);

    const outStream = fs.createWriteStream(outPath);
    const buffer = Buffer.alloc(PAGE_SIZE);
    let offset = 0;

    function writeVarint(value) {
        while (value >= 0x80) {
            buffer.writeUInt8((value & 0x7F) | 0x80, offset);
            value = value >>> 7;
            offset++;
        }
        buffer.writeUInt8(value & 0x7F, offset);
        offset++;
    }

    function writeBuffer(value) {
        writeVarint(value.length);
        const valueBuffer = Buffer.from(value);
        valueBuffer.copy(buffer, offset);
        offset += valueBuffer.length;
    }

    async function flushPage(accountId) {
        console.log('Writing', outPath, accountId, offset);

        // Fill the rest of the page with zeros
        buffer.fill(0, offset);

        const isLastPage = !accountId;
        await new Promise((resolve, reject) => {
            outStream.write(isLastPage ? buffer.subarray(0, offset) : buffer, e => e ? reject(e) : resolve());
        });
        offset = 0;

        if (!isLastPage) {
            writeBuffer(accountId);
        }
    }

    const sortedAccountIds = Object.keys(changesByAccount).sort();
    for (let accountId of sortedAccountIds) {
        const accountIdLength = Buffer.byteLength(accountId) + 2;
        if (offset + accountIdLength >= PAGE_SIZE) {
            await flushPage(accountId);
        } else {
            writeBuffer(accountId);
        }

        const accountChanges = changesByAccount[accountId];
        for (let { key, changes: allChanges } of accountChanges) {
            // NOTE: Changes arrays are split into chunks of up to 0xFF items
            // TODO: Use 0xFFFF instead of 0xFF
            const MAX_CHANGES_PER_RECORD = 0xFF;
            for (let i = 0; i < allChanges.length; ) {
                let changes = allChanges.slice(i, i + MAX_CHANGES_PER_RECORD);

                const keyLength = key.length + 2;
                const minChangesLength = 2 + 4 * 8; // 8 changes
                if (offset + keyLength + minChangesLength > PAGE_SIZE) {
                    await flushPage(accountId);
                }
                writeBuffer(key);

                // TODO: Calculate actual varint length
                const maxChangesLength = Math.floor((buffer.length - offset - 2) / 4);
                if (changes.length > maxChangesLength) {
                    changes = changes.slice(0, maxChangesLength);
                }
                writeVarint(changes.length);
                let prevChange = 0;
                for (let change of changes) {
                    writeVarint(change - prevChange);
                    prevChange = change;
                }
                i += changes.length;
            }
        }

        if (offset + 2 < PAGE_SIZE) {
            // Write zero length string to indicate no more keys for this account
            // If it doesn't fit page gonna be flushed on next iteration anyway
            writeBuffer('');
        }
    }

    await flushPage();
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

function changeKey(type, { public_key, key_base64 } ) {
    // TODO: Adjust this as needed
    switch (type) {
        case 'account_update':
        case 'account_deletion':
            return Buffer.from('a');
        case 'access_key_update':
        case 'access_key_deletion': {
            if (public_key.startsWith('ed25519:')) {
                return Buffer.concat([Buffer.from('e'), bs58.decode(public_key.slice(8))]);
            }
            return Buffer.from(`k${changeData.public_key}`);
        }
        case 'data_update':
        case 'data_deletion':
            return Buffer.concat([Buffer.from('d'), Buffer.from(key_base64, 'base64')]);
        case 'contract_code_update':
        case 'contract_code_deletion':
            return Buffer.from('c');
        default:
            throw new Error(`Unknown type ${type}`);
    }
}

function readPage(buffer) {
    let offset = 0;

    function readVarint() {
        const result = buffer.readUInt8(offset);
        offset++;
        if (result < 0x80) {
            return result;
        }

        let value = result & 0x7F;
        let shift = 7;
        while (true) {
            const byte = buffer.readUInt8(offset);
            offset++;
            value |= (byte & 0x7F) << shift;
            if (byte < 0x80) {
                return value;
            }
            shift += 7;
        }
    }

    function readBuffer() {
        // TODO: Is +2 correct here for varint length?
        if (offset + 2 >= PAGE_SIZE) {
            return null;
        }

        const length = readVarint();
        if (length === 0) {
            return null;
        }

        const result = buffer.slice(offset, offset + length);
        offset += length;
        return result;
    }

    function readString() {
        return readBuffer()?.toString('utf-8');
    }

    const result = [];
    let accountId;
    while (accountId = readString()) {
        let key;
        while (key = readBuffer()) {
            const count = readVarint();
            const changes = new Array(count);
            for (let i = 0; i < count; i++) {
                changes[i] = readVarint();
                if (i > 0) {
                    changes[i] += changes[i - 1];
                }
            }

            result.push({ accountId, key, changes });
        }
    }
    return result;
}

async function *readChangesFile(inPath) {
    const file = await open(inPath, 'r');

    const buffer = Buffer.alloc(PAGE_SIZE);

    let position = 0;
    let bytesRead;
    do {
        ({ bytesRead } = await file.read({ buffer, length: PAGE_SIZE, position }));
        buffer.fill(0, bytesRead);

        yield *readPage(buffer);

        position += PAGE_SIZE;
    } while (bytesRead === PAGE_SIZE);
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