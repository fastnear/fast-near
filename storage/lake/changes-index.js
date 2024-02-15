const fs = require('fs');
const { open } = require('fs/promises');

const { serialize } = require('borsh');
const bs58 = require('bs58');
const { BORSH_SCHEMA, Account, AccessKey, AccessKeyPermission, PublicKey, FunctionCallPermission, FullAccessPermission } = require('../../data-model');

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
                let prevChange = changes[0];
                writeVarint(prevChange);
                for (let change of changes.slice(1)) {
                    writeVarint(prevChange - change);
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

class BufferReader {
    constructor(buffer) {
        this.buffer = buffer;
        this.offset = 0;
    }

    readVarint() {
        const result = this.buffer.readUInt8(this.offset);
        this.offset++;
        if (result < 0x80) {
            return result;
        }

        let value = result & 0x7F;
        let shift = 7;
        while (true) {
            const byte = this.buffer.readUInt8(this.offset);
            this.offset++;
            value |= (byte & 0x7F) << shift;
            if (byte < 0x80) {
                return value;
            }
            shift += 7;
        }
    }

    readBuffer() {
        // TODO: Is +2 correct here for varint length?
        if (this.offset + 2 >= PAGE_SIZE) {
            return null;
        }

        const length = this.readVarint();
        if (length === 0) {
            return null;
        }

        const result = Buffer.from(this.buffer.subarray(this.offset, this.offset + length));
        this.offset += length;
        return result;
    }

    readString() {
        return this.readBuffer()?.toString('utf-8');
    }
}

function readPage(buffer) {
    const reader = new BufferReader(buffer);

    const result = [];
    let accountId;
    while (accountId = reader.readString()) {
        let key;
        while (key = reader.readBuffer()) {
            const count = reader.readVarint();
            const changes = new Array(count);
            for (let i = 0; i < count; i++) {
                changes[i] = reader.readVarint();
                if (i > 0) {
                    changes[i] = changes[i - 1] - changes[i];
                }
            }

            result.push({ accountId, key, changes });
        }
    }
    return result;
}

async function *readChangesFile(inPath, { accountId, keyPrefix, blockHeight } = {}) {
    const file = await open(inPath, 'r');
    try {
        const buffer = Buffer.alloc(PAGE_SIZE);
        let position = 0;

        if (accountId) {
            // Binary search for the account page
            const { size } = await file.stat();
            let left = 0;
            let right = Math.floor(size / PAGE_SIZE);
            while (left < right) {
                const mid = left + Math.floor((right - left) / 2);
                // TODO: Check if reading less than PAGE_SIZE helpful - in that case maybe decrease PAGE_SIZE?
                await file.read({ buffer, length: PAGE_SIZE, position: mid * PAGE_SIZE });

                const reader = new BufferReader(buffer);
                const midAccountId = reader.readString();

                let cmp = midAccountId > accountId ? 1 : (midAccountId < accountId ? -1 : 0);
                if (keyPrefix && cmp === 0) {
                    const key = reader.readBuffer();
                    cmp = -keyPrefix.compare(key.subarray(0, keyPrefix.length));
                    console.log('keyPrefix', keyPrefix, 'key', key, 'cmp', cmp);

                    if (cmp === 0 && blockHeight) {
                        const changesLength = reader.readVarint();
                        const firstChange = reader.readVarint();
                        console.log('accountId', accountId, 'keyPrefix', keyPrefix, 'blockHeight', blockHeight, 'firstChange', firstChange, 'changesLength', changesLength);
                        cmp = blockHeight - firstChange;
                    }
                }

                console.log('left', left, 'mid', mid, 'right', right, 'midAccountId', midAccountId, accountId);
                if (cmp < 0) {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }

            position = Math.max(0, (left - 1) * PAGE_SIZE);
        }

        let bytesRead;
        do {
            ({ bytesRead } = await file.read({ buffer, length: PAGE_SIZE, position }));
            buffer.fill(0, bytesRead);

            if (!accountId) {
                // TOOD: Shoud this also filter by block height?
                yield *readPage(buffer);
            } else {
                const items = readPage(buffer);
                console.log('readPage', position.toString(16),  items[0].accountId);
                for (let item of items) {
                    if (item.accountId === accountId) {
                        // TODO: Binary search for the key as well? Note that we already take it into account when searching for the page
                        if (keyPrefix) {
                            const { key } = item;
                            const cmp = keyPrefix.compare(key.subarray(0, keyPrefix.length));
                            if (cmp === 0) {
                                item = filterByBlockHeight(item, blockHeight);
                                if (item.changes.length > 0) {
                                    yield item;
                                }
                            }

                            if (cmp < 0) {
                                return;
                            }
                        } else {
                            item = filterByBlockHeight(item, blockHeight);
                            if (item.changes.length > 0) {
                                yield item;
                            }
                        }
                    }

                    if (item.accountId > accountId) {
                        return;
                    }
                }
            }

            position += PAGE_SIZE;
        } while (bytesRead === PAGE_SIZE);
    } finally {
        await file.close();
    }
}

function filterByBlockHeight(item, blockHeight) {
    if (!blockHeight) {
        return item;
    }

    const lastChange = item.changes.at(-1);
    if (blockHeight < lastChange) {
        return { ...item, changes: [] }
    }

    const index = item.changes.findIndex(change => change <= blockHeight);
    return { ...item, changes: item.changes.slice(index) };
}
function changeKey(type, { public_key, key_base64 } ) {
    // TODO: Adjust this as needed
    switch (type) {
        case 'account_update':
        case 'account_deletion':
            return Buffer.from('a');
        case 'access_key_update':
        case 'access_key_deletion': {
            return Buffer.concat([
                Buffer.from(`k`),
                serialize(BORSH_SCHEMA, PublicKey.fromString(public_key))
            ]);
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

function changeValue(type, data) {
    if (type.endsWith('_deletion')) {
        return null;
    }

    switch (type) {
        case 'account_update': {
            const { amount, code_hash, locked, storage_usage } = data;
            return serialize(BORSH_SCHEMA, new Account({amount, code_hash: bs58.decode(code_hash), locked, storage_usage }));
        }
        case 'access_key_update': {
            const { access_key: { nonce, permission } } = data;
            // NOTE: nonce.toString() is a hack to make stuff work, near-lake shouldn't use number for u64 values as it results in data loss
            const accessKey = new AccessKey({
                nonce: nonce.toString(),
                permission: new AccessKeyPermission(
                    permission === 'FullAccess'
                        ? { fullAccess: new FullAccessPermission() }
                        : { functionCall: new FunctionCallPermission(permission.FunctionCall) })
            });
            return serialize(BORSH_SCHEMA, accessKey);
        }
        case 'data_update': {
            const { value_base64 } = data;
            return Buffer.from(value_base64, 'base64');
        }
        case 'contract_code_update': {
            const { code_base64 } = data;
            return Buffer.from(code_base64, 'base64');
        }
        default:
            throw new Error(`Unknown type ${type}`);
    }
}

module.exports = {
    writeChangesFile,
    readChangesFile,
    changeKey,
    changeValue,
};