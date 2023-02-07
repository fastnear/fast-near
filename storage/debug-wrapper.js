const bs58 = require('bs58');
const debug = require('debug')('storage');

const prettyBuffer = require('../utils/pretty-buffer');
const sha256 = require('../utils/sha256');

class DebugStorage {
    constructor(storage) {
        this._storage = storage;
    }
    
    async getLatestBlockHeight() {
        debug('getLatestBlockHeight');
        try {
            return await this._storage.getLatestBlockHeight();
        } finally {
            debug('getLatestBlockHeight done');
        }
    }

    async setLatestBlockHeight(blockHeight) {
        debug('setLatestBlockHeight', blockHeight);
        try {
            return await this._storage.setLatestBlockHeight(blockHeight);
        } finally {
            debug('setLatestBlockHeight done');
       }
    }

    async getBlockTimestamp(blockHeight) {
        debug('getBlockTimestamp', blockHeight);
        try {
            return await this._storage.getBlockTimestamp(blockHeight);
        } finally {
            debug('getBlockTimestamp done');
        }
    }

    async setBlockTimestamp(blockHeight, timestamp) {
        debug('setBlockTimestamp', blockHeight, timestamp);
        try {
            return await this._storage.setBlockTimestamp(blockHeight, timestamp);
        } finally {
            debug('setBlockTimestamp done');
        }
    }
    
    async getLatestDataBlockHeight(compKey, blockHeight) {
        if (debug.enabled) {
            debug('getLatestDataBlockHeight', prettyBuffer(compKey), blockHeight);
        }
        try {
            return await this._storage.getLatestDataBlockHeight(compKey, blockHeight);
        } finally {
            debug('getLatestDataBlockHeight done');
        }
    }

    async getData(compKey, blockHeight) {
        if (debug.enabled) {
            debug('getData', prettyBuffer(compKey), blockHeight);
        }
        try {
            return await this._storage.getData(compKey, blockHeight);
        } finally {
            debug('getData done');
        }
    }

    async getLatestData(compKey, blockHeight) {
        if (debug.enabled) {
            debug('getLatestData', prettyBuffer(compKey), blockHeight);
        }
        try {
            return await this._storage.getLatestData(compKey, blockHeight);
        } finally {
            debug('getLatestData done');
        }
    }

    async setData(batch, scope, accountId, storageKey, blockHeight, data) {
        if (debug.enabled) {
            debug('setData', scope, accountId, prettyBuffer(storageKey), blockHeight, data.length, 'bytes');
        }
        try {
            return await this._storage.setData(batch, scope, accountId, storageKey, blockHeight, data);
        } finally {
            debug('setData done');
        }
    }

    async deleteData(batch, scope, accountId, storageKey, blockHeight) {
        if (debug.enabled) {
            debug('deleteData', scope, accountId, prettyBuffer(storageKey), blockHeight);
        }
        try {
            return await this._storage.deleteData(batch, scope, accountId, storageKey, blockHeight);
        } finally {
            debug('deleteData done');
        }
    }

    async getBlob(hash) {
        if (debug.enabled) {
            debug('getBlob', bs58.encode(hash));
        }
        try {
            return await this._storage.getBlob(hash);
        } finally {
            debug('getBlob done');
        }
    }

    async setBlob(batch, data) {
        if (debug.enabled) {
            // TODO: Refactor to avoid double hashing even in debug mode.
            const hash = sha256(data);
            const bs58hash = bs58.encode(hash);
            debug('setBlob', bs58hash, data.length, 'bytes')
        }
        try {
            return await this._storage.setBlob(batch, data);
        } finally {
            debug('setBlob done');
        }
    }

    async cleanOlderData(batch, key, blockHeight) {
        if (debug.enabled) {
            debug('cleanOlderData', prettyBuffer(key), blockHeight);
        }
        try {
            return await this._storage.cleanOlderData(batch, key, blockHeight);
        } finally {
            debug('cleanOlderData done');
        }
    }

    async scanAllKeys(iterator) {
        debug('scanAllKeys', iterator);
        try {
            return await this._storage.scanAllKeys(iterator);
        } finally {
            debug('scanAllKeys done');
        }
    }

    async scanDataKeys(contractId, blockHeight, keyPattern, iterator) {
        debug('scanDataKeys', contractId, blockHeight, keyPattern, iterator);
        try {
            return await this._storage.scanDataKeys(contractId, blockHeight, keyPattern, iterator);
        } finally {
            debug('scanDataKeys done');
        }
    }

    async writeBatch(fn) {
        debug('writeBatch');
        try {
            return await this._storage.writeBatch(fn);
        } finally {
            debug('writeBatch done');
        }
    }

    async clearDatabase() {
        debug('clearDatabase');
        try {
            return await this._storage.clearDatabase();
        } finally {
            debug('clearDatabase done');
        }
    }

    async closeDatabase() {
        debug('closeDatabase');
        try {
            return await this._storage.closeDatabase();
        } finally {
            debug('closeDatabase done');
        }
    }
}

module.exports = { DebugStorage };