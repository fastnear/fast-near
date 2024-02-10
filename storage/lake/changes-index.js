const fs = require('fs');
const { open } = require('fs/promises');

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

module.exports = {
    writeChangesFile,
    readChangesFile,
};