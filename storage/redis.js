const { createClient } = require('redis');
const { promisify } = require('util');
const crypto = require('crypto');

const debug = require('debug')('storage');

const LRU = require('lru-cache');

const prettyBuffer = require('../utils/pretty-buffer');
const { withTimeCounter } = require('../utils/counters');
const { compositeKey, allKeysKey, DATA_SCOPE } = require('../storage-keys');
const sha256 = require('../utils/sha256');

const REDIS_CACHE_MAX_ITEMS = 1000;
const BLOCK_INDEX_CACHE_TIME = 500;
const MAX_SCAN_STEPS = 10;
const SCAN_COUNT = 1000;
const REDIS_URL = process.env.FAST_NEAR_REDIS_URL || 'redis://localhost:6379';

const prettyArgs = args => args.map(arg => arg instanceof Uint8Array || arg instanceof Buffer ? prettyBuffer(arg) : `${arg}`);

function dataHistoryKey(compKey) {
    return Buffer.concat([Buffer.from('h:'), compKey]);
}

function dataKey(compKey, blockHeight) {
    return Buffer.concat([Buffer.from('d:'), compKey, Buffer.from(`:${blockHeight}`)]);
}

const blobKey = hash => Buffer.concat([Buffer.from('b:'), hash]);

// TODO: Split caching and storage logic? Caching logic can go into wrapping CachingStorage class
class RedisStorage {
    constructor({
        cacheMaxItems = REDIS_CACHE_MAX_ITEMS,
        blockIndexCacheTime = BLOCK_INDEX_CACHE_TIME,
        redisUrl = REDIS_URL
    } = {}) {
        this.redisCache = new LRU({
            max: cacheMaxItems,
        });

        const redisClient = createClient(REDIS_URL, {
            detect_buffers: true
        });
        // TODO: Does it need to crash as fatal error?
        redisClient.on('error', (err) => console.error('Redis Client Error', err));

        this.redisClient = {
            get: promisify(redisClient.get).bind(redisClient),
            set: promisify(redisClient.set).bind(redisClient),
            hset: promisify(redisClient.hset).bind(redisClient),
            del: promisify(redisClient.del).bind(redisClient),
            zadd: promisify(redisClient.zadd).bind(redisClient),
            zrem: promisify(redisClient.zrem).bind(redisClient),
            sendCommand: promisify(redisClient.sendCommand).bind(redisClient),
            scan: promisify(redisClient.scan).bind(redisClient),
            hscan: promisify(redisClient.hscan).bind(redisClient),
            quit: promisify(redisClient.quit).bind(redisClient),
            batch() {
                const batch = redisClient.batch();
                batch.exec = promisify(batch.exec).bind(batch);
                batch.redisClient = this;
                return batch;
            }
        };
    }

    withRedis = ({ name }, fn) => async (...args) => {
        const readableArgs = prettyArgs(args);
        debug(name, ...readableArgs);
        try {
            return await fn.call(this, ...args);
        } finally {
            debug(`${name} done`, ...readableArgs);
        }
    }

    withRedisAndCache = ({ name, cachedExpires }, fn) => async (...args) => {
        const readableArgs = prettyArgs(args);
        debug(name, ...readableArgs);
        try {
            let cacheKey = [name, ...readableArgs].join('$$');
            const cachedPromise = this.redisCache.get(cacheKey);
            if (cachedPromise) {
                debug(name, 'local cache hit', cacheKey);
                return await cachedPromise;
            }
            debug(name, 'local cache miss', cacheKey);

            const resultPromise = fn.call(this, ...args);
            // TODO: Protect from size-bombing cache?
            this.redisCache.set(cacheKey, resultPromise, cachedExpires && BLOCK_INDEX_CACHE_TIME);
            return await resultPromise;
        } finally {
            debug(`${name} done`, ...readableArgs);
        }
    }

    getLatestBlockHeight = this.withRedisAndCache({ name: 'getLatestBlockHeight', cachedExpires: true },
        async function () {
            return await this.redisClient.get('latest_block_height');
        });

    setLatestBlockHeight = this.withRedis({ name: 'setLatestBlockHeight' },
        async function (blockHeight) {
            this.redisCache.del('getLatestBlockHeight');
            return await this.redisClient.set('latest_block_height', blockHeight.toString());
        });

    getBlockTimestamp = this.withRedisAndCache({ name: 'getBlockTimestamp' },
        async function (blockHeight) {
            return await this.redisClient.get(`t:${blockHeight}`);
        });

    setBlockTimestamp = this.withRedis({ name: 'setBlockTimestamp' },
        async function (blockHeight, blockTimestamp) {
            return await this.redisClient.set(`t:${blockHeight}`, blockTimestamp);
        });

    getLatestDataBlockHeight = this.withRedisAndCache({ name: 'getLatestDataBlockHeight' },
        async function (compKey, blockHeight) {
            compKey = Buffer.from(compKey);
            const [dataBlockHeight] = await this.redisClient.sendCommand('ZREVRANGEBYSCORE',
                [dataHistoryKey(compKey), blockHeight, '-inf', 'LIMIT', '0', '1']);
            return dataBlockHeight;
        });

    getData = this.withRedisAndCache({ name: 'getData' },
        async function (compKey, blockHeight) {
            compKey = Buffer.from(compKey);
            return await this.redisClient.get(dataKey(compKey, blockHeight));
        });

    async getLatestData(compKey, blockHeight) {
        const dataBlockHeight = await this.getLatestDataBlockHeight(compKey, blockHeight);
        if (!dataBlockHeight) {
            return null;
        }
        return await this.getData(compKey, dataBlockHeight);
    }

    // TODO: Encode blockHeight more efficiently than string? int32 should be enough for more than 20 years.
    async setData(batch, scope, accountId, storageKey, blockHeight, data) {
        debug('setData', ...prettyArgs([scope, accountId, storageKey, blockHeight]));
        const compKey = compositeKey(scope, accountId, storageKey);
        batch
            .set(dataKey(compKey, blockHeight), data)
            .zadd(dataHistoryKey(compKey), blockHeight, blockHeight);

        if (storageKey) {
            batch.hset(allKeysKey(scope, accountId), storageKey, blockHeight);
        }
    }

    async deleteData(batch, scope, accountId, storageKey, blockHeight) {
        const compKey = compositeKey(scope, accountId, storageKey);
        batch
            .zadd(dataHistoryKey(compKey), blockHeight, blockHeight);
        if (storageKey) {
            batch.hset(allKeysKey(scope, accountId), storageKey, blockHeight);
        }
    }

    getBlob = this.withRedisAndCache({ name: 'getBlob' },
        async function (hash) {
            return await this.redisClient.get(blobKey(hash));
        });

    async setBlob(batch, data) {
        const hash = sha256(data);
        batch.set(blobKey(hash), data);
        return hash;
    }

    // TODO: Garbage collection for blobs?

    async cleanOlderData(batch, compKey, blockHeight) {
        const redisClient = batch.redisClient;
        await withTimeCounter('cleanOlderData', async () => {
            compKey = Buffer.from(compKey);
            const blockHeightKey = dataHistoryKey(compKey);
            const blockHeights = await withTimeCounter('cleanOlderData:range', () => redisClient.sendCommand('ZREVRANGEBYSCORE', [blockHeightKey, blockHeight, '-inf']));
            let hightsToRemove = blockHeights.slice(1);
            const BATCH_SIZE = 100000;
            while (hightsToRemove.length > 0) {
                const removeBatch = hightsToRemove.slice(0, BATCH_SIZE);
                batch
                    .del(removeBatch.map(blockHeight => dataKey(compKey, blockHeight)))
                    .zrem(blockHeightKey, removeBatch);
                hightsToRemove = hightsToRemove.slice(BATCH_SIZE);
            }
        });
    }

    scanAllKeys = this.withRedis({ name: 'scanAllKeys' },
        async function (iterator) {
            const [newIterator, keys] = await this.redisClient.scan(iterator || 0, 'MATCH', Buffer.from('h:*'), 'COUNT', SCAN_COUNT);
            return [newIterator, keys.map(k =>
                k.slice(2) // NOTE: Remove h: prefix
            )];
        });

    // TODO: Does this work ok with caching???
    scanDataKeys = this.withRedisAndCache({ name: 'scanDataKeys' },
        async function (contractId, blockHeight, keyPattern, iterator, limit) {
            let step = 0;
            let data = [];
            do {
                const [newIterator, keys] = await this.redisClient.hscan(Buffer.from(`k:${DATA_SCOPE}:${contractId}`), iterator, 'MATCH', keyPattern, 'COUNT', SCAN_COUNT);
                console.log('keys', keys.map(k => k.toString('utf8')), newIterator.toString('utf8'))
                const newData = await Promise.all(keys.map(async storageKey => {
                    const compKey = Buffer.concat([Buffer.from(`${DATA_SCOPE}:${contractId}:`), storageKey]);
                    const dataBlockHeight = await this.getLatestDataBlockHeight(compKey, blockHeight);
                    if (!dataBlockHeight) {
                        return [storageKey, null];
                    }
                    return [storageKey, await this.getData(compKey, dataBlockHeight)];
                }));
                iterator = newIterator;
                data = data.concat(newData);
                step++;
                console.log('step', step, 'iterator', iterator.toString('utf8'));
            } while (step < MAX_SCAN_STEPS && data.length < limit && iterator.toString('utf8') != '0');
            return {
                iterator: Buffer.from(iterator).toString('utf8'),
                data
            };
        });

    async writeBatch(fn) {
        await this.withRedis({ name: 'batch' }, async () => {
            const batch = this.redisClient.batch();
            await fn(batch);
            await batch.exec();
        })();
    }

    async clearDatabase() {
        await this.redisClient.sendCommand('FLUSHDB');
        this.redisCache.reset();
    }

    async closeDatabase() {
        return this.redisClient.quit();
    }
}

module.exports = { RedisStorage };