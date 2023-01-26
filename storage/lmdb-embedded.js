const { open } = require('lmdb');
const bs58 = require('bs58');
const sha256 = require('../utils/sha256');
const { compositeKey, DATA_SCOPE } = require('../storage-keys');

const LMDB_PATH = process.env.FAST_NEAR_LMDB_PATH || './lmdb-data';

const KEY_TYPE_STRING = 0;
const KEY_TYPE_BUFFER = 1;
const KEY_TYPE_CHANGE = 2;

// TODO: Have debug wrapper storage class?
const debug = require('debug')('storage-lmdb-embedded');

class KeyEncoder {
    writeKey(key, targetBuffer, startPosition) {
        let offset = startPosition;
        if (typeof key === 'string') {
            offset = targetBuffer.writeUInt8(KEY_TYPE_STRING, offset);
            offset += targetBuffer.write(key, offset);
        } else if (Buffer.isBuffer(key)) {
            offset = targetBuffer.writeUInt8(KEY_TYPE_BUFFER, offset);
            offset += key.copy(targetBuffer, offset);
        } else if (key.blockHeight !== undefined && key.compKey) {
            offset = targetBuffer.writeUInt8(KEY_TYPE_CHANGE, offset);
            offset += key.compKey.copy(targetBuffer, offset);
            offset = targetBuffer.writeUInt32LE(key.blockHeight, offset);
        } else {
            throw new Error(`Unsupported key type: ${typeof key}, key: ${JSON.stringify(key)}`);
        }

        return offset;
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
                const compKey = buffer.slice(offset, end - 4);
                const blockHeight = buffer.readUInt32LE(end - 4);
                return { compKey, blockHeight };
            default:
                throw new Error('Unsupported key type: ' + type);
        }
    }
}

const keyEncoder = new KeyEncoder();

class LMDBStorage {
    constructor() {
        this.db = open({
            path: LMDB_PATH,
            keyEncoder,
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
        debug('getLatestDataBlockHeight', JSON.stringify(compKey.toString('utf8')), blockHeight);
        const [latest] = this.db.getKeys({
            start: { compKey, blockHeight },
            end: { compKey, blockHeight: 0 },
            limit: 1,
            reverse: true,
        }).asArray;

        debug('latest', latest);
        if (latest) {
            debug('1', latest.blockHeight);
            return latest.blockHeight;
        }

        debug('2');
        return null;
    }

    async getData(compKey, blockHeight) {
        debug('getData', JSON.stringify(compKey.toString('utf8')), blockHeight);
        const result = this.db.get({ compKey, blockHeight });
        return result && Buffer.from(result);
    }

    async getLatestData(compKey, blockHeight) {
        debug('getLatestData', JSON.stringify(compKey.toString('utf8')), blockHeight);
        const dataBlockHeight = await this.getLatestDataBlockHeight(compKey, blockHeight);
        debug('dataBlockHeight', dataBlockHeight);
        if (!dataBlockHeight) {
            return null;
        }

        return await this.getData(compKey, dataBlockHeight);
    }

    setData(batch, scope, accountId, storageKey, blockHeight, data) {
        const compKey = compositeKey(scope, accountId, storageKey);
        debug('setData', JSON.stringify(compKey.toString('utf8')), blockHeight, data.length, 'bytes');
        this.db.put({ compKey, blockHeight }, data);
    }

    deleteData(batch, scope, accountId, storageKey, blockHeight) {
        const compKey = compositeKey(scope, accountId, storageKey);
        this.db.put({ compKey, blockHeight }, null);
    }

    async getBlob(hash) {
        const result = this.db.get(`b:${bs58.encode(hash)}`);
        return result && Buffer.from(result);
    }

    setBlob(batch, data) {
        const hash = sha256(data);
        debug('setBlob', bs58.encode(hash), data.length, 'bytes');
        this.db.put(`b:${bs58.encode(hash)}`, data);
    }

    cleanOlderData(batch, key, blockHeight) {
        // TODO: Is it still needed?
    }

    scanAllKeys(iterator) {
        // TODO: Is it still needed?
    }

    scanDataKeys(contractId, blockHeight, keyPattern, iterator, limit) {
        iterator = iterator || '0';
        // TODO: More robust pattern handling
        const keyPrefix = keyPattern.replace(/\*$/, '');

        let start; 
        if (iterator === '0') {
            start = { blockHeight, compKey: compositeKey(DATA_SCOPE, contractId, keyPrefix) };
        } else {
            const buffer = Buffer.from(iterator, 'hex');
            start = keyEncoder.readKey(buffer, 0, buffer.length);
        }

        const data = this.db.getRange({
            start,
            end: { blockHeight: 0xffffffff, compKey: compositeKey(DATA_SCOPE, contractId, keyPrefix + '\xff') },
            limit,
        }).asArray;
        
        if (data.length > 0) {
            // compute serialized key using writeKey
            const buffer = Buffer.alloc(1024);
            const offset = keyEncoder.writeKey(data[0].key, buffer, 0);
            const serializedKey = buffer.slice(0, offset);
            iterator = serializedKey.toString('hex');
        }

        if (data.length < limit) {
            iterator = '0';
        }

        return {
            iterator,
            data: data.map(({ key, value }) => {
                const storageKey = key.compKey.slice(3 + contractId.length);
                return [
                    storageKey,
                    value,
                ];
            }),
        };
    }

    async writeBatch(fn) {
        // TODO: Is any real implementation needed?
        await this.db.transaction(() => {
            fn();
        });
    }

    async clearDatabase() {
        return this.db.drop();
    }

    async closeDatabase() {
        return this.db.close();
    }

    async printDatabase() {
        console.log('printDatabase');
        this.db.getRange().forEach(({ key, value }) => {
            if (key.compKey) {
                console.log(JSON.stringify(key.compKey.toString('utf8')), key.blockHeight, '->', value);
            } else {
                console.log(key, '->', value);
            }
        });
    }
}

module.exports = { LMDBStorage, KeyEncoder };