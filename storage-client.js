const { createClient } = require('redis');
const { promisify } = require("util");

const debug = require('debug')('storage');

const LRU = require("lru-cache");
let redisCache = new LRU({
    max: 1000
});

const BLOCK_INDEX_CACHE_TIME = 500;
const REDIS_URL = process.env.FAST_NEAR_REDIS_URL || 'redis://localhost:6379';

const SCAN_COUNT = 100000;

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
        del: promisify(redisClient.del).bind(redisClient),
        zadd: promisify(redisClient.zadd).bind(redisClient),
        zrem: promisify(redisClient.zrem).bind(redisClient),
        sendCommand: promisify(redisClient.sendCommand).bind(redisClient),
        scan: promisify(redisClient.scan).bind(redisClient),
        batch() {
            const batch = redisClient.batch();
            batch.exec = promisify(batch.exec).bind(batch);
            batch.redisClient = this;
            return batch;
        }
    };
}

const prettyBuffer = require('./pretty-buffer');
const { withTimeCounter } = require('./counters');

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

function dataBlockHashKey(compKey) {
    return Buffer.concat([Buffer.from('h:'), compKey]);
}

function dataKey(compKey, blockHash) {
    return Buffer.concat([Buffer.from('d:'), compKey, Buffer.from(':'), blockHash]);
}

const getLatestDataBlockHash = redisClient => async (compKey, blockHeight) => {
    compKey = Buffer.from(compKey);
    const [blockHash] = await redisClient.sendCommand('ZREVRANGEBYSCORE',
        [dataBlockHashKey(compKey), blockHeight, '-inf', 'LIMIT', '0', '1']);
    return blockHash;
};

const getData = redisClient => async (compKey, blockHash) => {
    compKey = Buffer.from(compKey);
    return await redisClient.get(dataKey(compKey, blockHash));
};

const setData = batch => (compKey, blockHash, blockHeight, data) => {
    compKey = Buffer.from(compKey);
    batch
        .set(dataKey(compKey, blockHash), data)
        .zadd(dataBlockHashKey(compKey), blockHeight, blockHash);
};

const deleteData = batch => async (compKey, blockHash, blockHeight) => {
    compKey = Buffer.from(compKey);
    batch
        .zadd(dataBlockHashKey(compKey), blockHeight, blockHash);
};

const cleanOlderData = batch => async (compKey, blockHeight) => {
    const redisClient = batch.redisClient;
    await withTimeCounter('cleanOlderData', async () => {
        compKey = Buffer.from(compKey);
        const blockHashKey = dataBlockHashKey(compKey);
        const blockHashes = await withTimeCounter('cleanOlderData:range', () => redisClient.sendCommand('ZREVRANGEBYSCORE', [blockHashKey, blockHeight, '-inf']));
        let hashesToRemove = blockHashes.slice(1);
        const BATCH_SIZE = 100000;
        while (hashesToRemove.length > 0) {
            const removeBatch = hashesToRemove.slice(0, BATCH_SIZE);
            batch
                .del(removeBatch.map(blockHash => dataKey(compKey, blockHash)))
                .zrem(blockHashKey, removeBatch);
            hashesToRemove = hashesToRemove.slice(BATCH_SIZE);
        }
    });
}

const scanAllKeys = redisClient => async (iterator) => {
    const [newIterator, keys] = await redisClient.scan(iterator || 0, 'MATCH', Buffer.from('h:*'), 'COUNT', SCAN_COUNT);
    return [newIterator, keys.map(k =>
        k.slice(2) // NOTE: Remove h: prefix
    )];
}

const scanDataKeys = redisClient => async (contractId, blockHeight, keyPattern, iterator, limit) => {
    const [newIterator, keys] = await redisClient.scan(iterator, 'MATCH', Buffer.from(`data:${contractId}:${keyPattern}`), 'COUNT', limit);
    const data = await Promise.all(keys.map(async key => {
        const compKey = Buffer.from(key).slice('data:'.length);
        const storageKey = compKey.slice(contractId.length + 1);
        const blockHash = await module.exports.getLatestDataBlockHash(compKey, blockHeight);
        if (!blockHash) {
            return [storageKey, null];
        }
        return [storageKey, await module.exports.getData(compKey, blockHash)];
    }));
    return {
        iterator: Buffer.from(newIterator).toString('utf8'),
        data
    };
};

const redisBatch = async (fn) => {
    await withRedis({ name: 'batch' }, redisClient => async () => {
        const batch = redisClient.batch();
        await fn(batch);
        await batch.exec();
    })();
};

const clearDatabase = redisClient => async () => {
    console.log('clearDatabase');
    await redisClient.sendCommand('FLUSHDB');
};

const closeRedis = () => new Promise((resolve, reject) => redisClient.quit(e => e ? reject(e) : resolve()));

module.exports = {
    // TODO: Rely on function name instead?
    getLatestBlockHeight: withRedisAndCache({ name: 'getLatestBlockHeight', cachedExpires: true }, getLatestBlockHeight),
    getBlockTimestamp: withRedisAndCache({ name: 'getBlockTimestamp' }, getBlockTimestamp),
    getLatestDataBlockHash: withRedisAndCache({ name: 'getLatestDataBlockHash' }, getLatestDataBlockHash),
    getData: withRedisAndCache({ name: 'getData' }, getData),
    scanDataKeys: withRedisAndCache({ name: 'scanDataKeys' }, scanDataKeys),
    setLatestBlockHeight: withRedis({ name: 'setLatestBlockHeight' }, setLatestBlockHeight),
    setBlockTimestamp: withRedis({ name: 'setBlockTimestamp' }, setBlockTimestamp),
    scanAllKeys: withRedis({ name: 'scanAllKeys' }, scanAllKeys),
    setData,
    deleteData,
    cleanOlderData,
    redisBatch,
    closeRedis,
    clearDatabase: withRedis({ name: 'clearDatabase' }, clearDatabase),
};
