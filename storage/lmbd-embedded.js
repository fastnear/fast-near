const { open } = require('lmdb');

const LMDB_PATH = process.env.FAST_NEAR_LMDB_PATH || './lmdb-data';

const KEY_TYPE_STRING = 0;
const KEY_TYPE_BUFFER = 1;
const KEY_TYPE_CHANGE = 2;

class KeyEncoder {
    writeKey(key, targetBuffer, startPosition) {
        if (typeof key === 'string') {
            targetBuffer.writeUInt8(KEY_TYPE_STRING, startPosition);
            return startPosition + targetBuffer.write(key, startPosition + 1) + 1;
        }

        if (Buffer.isBuffer(key)) {
            targetBuffer.writeUInt8(KEY_TYPE_BUFFER, startPosition);
            return startPosition + key.copy(targetBuffer, startPosition + 1) + 1;
        }

        if (key.blockHeight && key.compKey) {
            startPosition += targetBuffer.writeUInt8(KEY_TYPE_CHANGE, startPosition);
            startPosition += targetBuffer.writeUInt32LE(key.compKey.length, startPosition);
            startPosition += key.compKey.copy(targetBuffer, startPosition);
            startPosition += targetBuffer.writeUInt32LE(key.blockHeight, startPosition);
        }

        throw new Error('Unsupported key type: ' + typeof key);
    }

    readKey(buffer, start, end) {
        let offset = start;
        const type = buffer.readUInt8(offset);
        offset += 1;
        switch (type) {
            case KEY_TYPE_STRING:
                return buffer.toString('utf8', offset, end);
            case KEY_TYPE_BUFFER:
                return buffer.slice(offset, end);
            case KEY_TYPE_CHANGE:
                const compKeyLength = buffer.readUInt32LE(offset);
                offset += 4;
                const compKey = buffer.slice(offset, offset + compKeyLength);
                offset += compKeyLength;
                const blockHeight = buffer.readUInt32LE(offset);
                offset += 4;
                return { compKey, blockHeight };
            default:
                throw new Error('Unsupported key type: ' + type);
        }
    }
}

class LMDBStorage {
    constructor() {
        this.db = open({
            path: LMDB_PATH,
            keyEncoder: new KeyEncoder(),
            // compression: true, // TODO: Check if this is worth it
        });
    }

    async getLatestBlockHeight() {
        return this.db.get('latest_block_height');
    }

    async setLatestBlockHeight(blockHeight) {
        return this.db.put('latest_block_height', blockHeight);
    }

    async getBlockTimestamp(blockHeight) {
        return this.db.get(`t:${blockHeight}`);
    }

    async setBlockTimestamp(blockHeight, timestamp) {
        return this.db.put(`t:${blockHeight}`, timestamp);
    }

    async getLatestDataBlockHeight(compKey, blockHeight) {
        // TODO: Implement using getRange
        this.db.getRange({
            start: { compKey, blockHeight },
            end: { compKey, blockHeight: 0 },
            limit: 1,
            reverse: true,
        }).forEach(({ key }) => {
            return key.blockHeight;
        });  

        return null;
    }

    async getData(compKey, blockHeight) {
        return this.db.get({ compKey, blockHeight });
    }

    async getLatestData(compKey, blockHeight) {
        const dataBlockHeight = await this.getLatestDataBlockHeight(compKey, blockHeight);
        if (!dataBlockHeight) {
            return null;
        }

        return await this.getData(compKey, dataBlockHeight);
    }

    async setData(batch, scope, accountId, storageKey, blockHeight, data) {
        //const compKey = `${scope}:${accountId}:${storageKey}`;
        // await batch.put({ compKey, blockHeight }, data);
        // TODO
    }

}