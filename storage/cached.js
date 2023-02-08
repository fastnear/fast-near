const LRU = require('lru-cache');
const debug = require('debug')('storage:cache');

const prettyBuffer = require('../utils/pretty-buffer');

const CACHE_MAX_ITEMS = parseInt(process.env.FAST_NEAR_CACHE_MAX_ITEMS || '1000');
const BLOCK_INDEX_CACHE_TIME = parseInt(process.env.FAST_NEAR_BLOCK_INDEX_CACHE_TIME || '500');

const prettyArgs = args => args.map(arg => arg instanceof Uint8Array || arg instanceof Buffer ? prettyBuffer(arg) : `${arg}`);

class CachedStorage {
    constructor(storage, {
            cacheMaxItems = CACHE_MAX_ITEMS,
            blockIndexCacheTime = BLOCK_INDEX_CACHE_TIME,
        } = {}) {
        this.cachedStorage = storage;
        this.cache = new LRU({
            max: cacheMaxItems,
        });
        this.blockIndexCacheTime = blockIndexCacheTime;
    }

    loadCachedOrFetch({ name, fn, args, cachedExpires = false }) {
        const readableArgs = prettyArgs(args);
        let cacheKey = [name, ...readableArgs].join('$$');
        const cachedPromise = this.cache.get(cacheKey);
        if (cachedPromise) {
            debug(name, 'local cache hit', cacheKey);
            return cachedPromise;
        }
        debug(name, 'local cache miss', cacheKey);

        const promise = fn.call(this.cachedStorage, ...args);
        // TODO: Protect from size-bombing cache?
        this.cache.set(cacheKey, promise, cachedExpires && this.blockIndexCacheTime);
        return promise;
    }

    getLatestBlockHeight() {
        return this.loadCachedOrFetch({
            name: 'getLatestBlockHeight',
            fn: this.cachedStorage.getLatestBlockHeight,
            args: [],
            cachedExpires: true
        });
    }

    setLatestBlockHeight(blockHeight) {
        this.cache.del('getLatestBlockHeight');
        return this.cachedStorage.setLatestBlockHeight(blockHeight);
    }

    getBlockTimestamp(blockHeight) {
        return this.loadCachedOrFetch({
            name: 'getBlockTimestamp',
            fn: this.cachedStorage.getBlockTimestamp,
            args: [blockHeight]
        });
    }
    
    setBlockTimestamp(blockHeight, timestamp) {
        return this.cachedStorage.setBlockTimestamp(blockHeight, timestamp);
    }

    getLatestDataBlockHeight(compKey, blockHeight) {
        return this.loadCachedOrFetch({
            name: 'getLatestDataBlockHeight',
            fn: this.cachedStorage.getLatestDataBlockHeight,
            args: [compKey, blockHeight]
        });
    }

    getData(compKey, blockHeight) {
        return this.loadCachedOrFetch({
            name: 'getData',
            fn: this.cachedStorage.getData,
            args: [compKey, blockHeight]
        });
    }

    getLatestData(compKey, blockHeight) {
        return this.loadCachedOrFetch({
            name: 'getLatestData',
            fn: this.cachedStorage.getLatestData,
            args: [compKey, blockHeight]
        });
    }

    setData(batch, scope, accountId, storageKey, blockHeight, data) {
        return this.cachedStorage.setData(batch, scope, accountId, storageKey, blockHeight, data);
    }

    deleteData(batch, scope, accountId, storageKey, blockHeight) {
        return this.cachedStorage.deleteData(batch, scope, accountId, storageKey, blockHeight);
    }

    getBlob(hash) {
        return this.loadCachedOrFetch({
            name: 'getBlob',
            fn: this.cachedStorage.getBlob,
            args: [hash]
        });
    }

    setBlob(batch, data) {
        return this.cachedStorage.setBlob(batch, data);
    }

    cleanOlderData(batch, compKey, blockHeight) {
        return this.cachedStorage.cleanOlderData(batch, compKey, blockHeight);
    }

    // NOTE: No caching as it's not used to serve requests
    scanAllKeys(iterator) {
        return this.cachedStorage.scanAllKeys(iterator);
    }

    // TODO: Does this work ok with caching???
    scanDataKeys(contractId, blockHeight, keyPattern, iterator, limit) {
        return this.loadCachedOrFetch({
            name: 'scanDataKeys',
            fn: this.cachedStorage.scanDataKeys,
            args: [contractId, blockHeight, keyPattern, iterator, limit]
        });
    }

    writeBatch(batch) {
        return this.cachedStorage.writeBatch(batch);
    }

    clearDatabase() {
        this.cache.reset();
        return this.cachedStorage.clearDatabase();
    }

    closeDatabase() {
        return this.cachedStorage.closeDatabase();
    }
}

module.exports = { CachedStorage }