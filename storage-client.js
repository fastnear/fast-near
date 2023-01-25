const { createClient } = require('redis');
const { promisify } = require("util");

const debug = require('debug')('storage');

const LRU = require("lru-cache");
let redisCache = new LRU({
    max: 1000
});

const BLOCK_INDEX_CACHE_TIME = 500;
const REDIS_URL = process.env.FAST_NEAR_REDIS_URL || 'redis://localhost:6379';

let redisClient;
function getRedisClient() {
    if (!redisClient) {
        redisClient = createClient(REDIS_URL, {
            detect_buffers: true
        });
        redisClient.on('error', (err) => console.error('Redis Client Error', err));
    }

    return {
        get: promisify(redisClient.get).bind(redisClient),
        set: promisify(redisClient.set).bind(redisClient),
        hset: promisify(redisClient.hset).bind(redisClient),
        del: promisify(redisClient.del).bind(redisClient),
        zadd: promisify(redisClient.zadd).bind(redisClient),
        zrem: promisify(redisClient.zrem).bind(redisClient),
        sendCommand: promisify(redisClient.sendCommand).bind(redisClient),
        scan: promisify(redisClient.scan).bind(redisClient),
        hscan: promisify(redisClient.hscan).bind(redisClient),
        batch() {
            const batch = redisClient.batch();
            batch.exec = promisify(batch.exec).bind(batch);
            batch.redisClient = this;
            return batch;
        }
    };
}

// TODO: Encode blockHeight more efficiently than string? int32 should be enough for more than 20 years.

const prettyBuffer = require('./pretty-buffer');
const { withTimeCounter } = require('./counters');
const { compositeKey, allKeysKey, DATA_SCOPE } = require('./storage-keys');

const prettyArgs = args => args.map(arg => arg instanceof Uint8Array || arg instanceof Buffer ? prettyBuffer(arg) : `${arg}`);

const withRedis = ({ name }, fn) => async (...args) => {
    const readableArgs = prettyArgs(args);
    debug(name, ...readableArgs);
    try {
        const redisClient = getRedisClient();
        return await fn(redisClient)(...args);
    } finally {
        debug(`${name} done`, ...readableArgs);
    }
}

const withRedisAndCache = ({ name, cachedExpires }, fn) => async (...args) => {
    const readableArgs = prettyArgs(args);
    debug(name, ...readableArgs);
    try {
        let cacheKey = [name, ...readableArgs].join('$$');
        const cachedPromise = redisCache.get(cacheKey);
        if (cachedPromise) {
            debug(name, 'local cache hit', cacheKey);
            return await cachedPromise;
        }
        debug(name, 'local cache miss', cacheKey);

        const redisClient = getRedisClient();
        const resultPromise = fn(redisClient)(...args);
        // TODO: Protect from size-bombing cache?
        redisCache.set(cacheKey, resultPromise, cachedExpires && BLOCK_INDEX_CACHE_TIME);
        return await resultPromise;
    } finally {
        debug(`${name} done`, ...readableArgs);
    }
}

const getLatestBlockHeight = redisClient => async () => {
    const result = await redisClient.get('latest_block_height');
    return result;
};

const setLatestBlockHeight = redisClient => async (blockHeight) => {
    redisCache.del('getLatestBlockHeight');
    return await redisClient.set('latest_block_height', blockHeight.toString());
}

const getBlockTimestamp = redisClient => async (blockHeight) => {
    return await redisClient.get(`t:${blockHeight}`);
}

const setBlockTimestamp = redisClient => async (blockHeight, blockTimestamp) => {
    return await redisClient.set(`t:${blockHeight}`, blockTimestamp);
}

function dataHistoryKey(compKey) {
    return Buffer.concat([Buffer.from('h:'), compKey]);
}

function dataKey(compKey, blockHeight) {
    return Buffer.concat([Buffer.from('d:'), compKey, Buffer.from(`:${blockHeight}`)]);
}

const getLatestDataBlockHeight = redisClient => async (compKey, blockHeight) => {
    compKey = Buffer.from(compKey);
    const [dataBlockHeight] = await redisClient.sendCommand('ZREVRANGEBYSCORE',
        [dataHistoryKey(compKey), blockHeight, '-inf', 'LIMIT', '0', '1']);
    return dataBlockHeight;
};

const getData = redisClient => async (compKey, blockHeight) => {
    compKey = Buffer.from(compKey);
    return await redisClient.get(dataKey(compKey, blockHeight));
};

const getLatestData = async (compKey, blockHeight) => {
    const dataBlockHeight = await module.exports.getLatestDataBlockHeight(compKey, blockHeight);
    if (!dataBlockHeight) {
        return null;
    }
    return await module.exports.getData(compKey, dataBlockHeight);
};

const setData = batch => (scope, accountId, storageKey, blockHeight, data) => {
    debug('setData', ...prettyArgs([scope, accountId, storageKey, blockHeight]));
    const compKey = compositeKey(scope, accountId, storageKey);
    batch
        .set(dataKey(compKey, blockHeight), data)
        .zadd(dataHistoryKey(compKey), blockHeight, blockHeight);

    if (storageKey) {
        batch.hset(allKeysKey(scope, accountId), storageKey, blockHeight);
    }
};

const deleteData = batch => async (scope, accountId, storageKey, blockHeight) => {
    const compKey = compositeKey(scope, accountId, storageKey);
    batch
        .zadd(dataHistoryKey(compKey), blockHeight, blockHeight);
    if (storageKey) {
        batch.hset(allKeysKey(scope, accountId), storageKey, blockHeight);
    }
};

const cleanOlderData = batch => async (compKey, blockHeight) => {
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

const scanAllKeys = redisClient => async (iterator) => {
    const [newIterator, keys] = await redisClient.scan(iterator || 0, 'MATCH', Buffer.from('h:*'), 'COUNT', SCAN_COUNT);
    return [newIterator, keys.map(k =>
        k.slice(2) // NOTE: Remove h: prefix
    )];
}

const MAX_SCAN_STEPS = 10;
const SCAN_COUNT = 1000;
// TODO: Does this work ok with caching???
const scanDataKeys = redisClient => async (contractId, blockHeight, keyPattern, iterator, limit) => {
    let step = 0;
    let data = [];
    do {
        const [newIterator, keys] = await redisClient.hscan(Buffer.from(`k:${DATA_SCOPE}:${contractId}`), iterator, 'MATCH', keyPattern, 'COUNT', SCAN_COUNT);
        console.log('keys', keys.map(k => k.toString('utf8')), newIterator.toString('utf8'))
        const newData = await Promise.all(keys.map(async storageKey => {
            const compKey = Buffer.concat([Buffer.from(`${DATA_SCOPE}:${contractId}:`), storageKey]);
            const dataBlockHeight = await module.exports.getLatestDataBlockHeight(compKey, blockHeight);
            if (!dataBlockHeight) {
                return [storageKey, null];
            }
            return [storageKey, await module.exports.getData(compKey, dataBlockHeight)];
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
};

const writeBatch = async (fn) => {
    await withRedis({ name: 'batch' }, redisClient => async () => {
        const batch = redisClient.batch();
        await fn(batch);
        await batch.exec();
    })();
};

const clearDatabase = redisClient => async () => {
    await redisClient.sendCommand('FLUSHDB');
    redisCache.reset();
};

const closeDatabase = () => new Promise((resolve, reject) => redisClient.quit(e => e ? reject(e) : resolve()));

module.exports = {
    // TODO: Rely on function name instead?
    getLatestBlockHeight: withRedisAndCache({ name: 'getLatestBlockHeight', cachedExpires: true }, getLatestBlockHeight),
    getBlockTimestamp: withRedisAndCache({ name: 'getBlockTimestamp' }, getBlockTimestamp),
    getLatestDataBlockHeight: withRedisAndCache({ name: 'getLatestDataBlockHeight' }, getLatestDataBlockHeight),
    getLatestData,
    getData: withRedisAndCache({ name: 'getData' }, getData),
    scanDataKeys: withRedisAndCache({ name: 'scanDataKeys' }, scanDataKeys),
    setLatestBlockHeight: withRedis({ name: 'setLatestBlockHeight' }, setLatestBlockHeight),
    setBlockTimestamp: withRedis({ name: 'setBlockTimestamp' }, setBlockTimestamp),
    scanAllKeys: withRedis({ name: 'scanAllKeys' }, scanAllKeys),
    setData,
    deleteData,
    cleanOlderData,
    writeBatch,
    closeDatabase,
    clearDatabase: withRedis({ name: 'clearDatabase' }, clearDatabase),
};
