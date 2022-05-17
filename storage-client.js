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
    };
}

const prettyBuffer = require('./pretty-buffer');

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

const setData = redisClient => async (compKey, blockHash, blockHeight, data) => {
    compKey = Buffer.from(compKey);
    await redisClient
        .set(dataKey(compKey, blockHash), data);
    await redisClient
        .zadd(dataBlockHashKey(compKey), blockHeight, blockHash);
};

const deleteData = redisClient => async (compKey, blockHash, blockHeight) => {
    compKey = Buffer.from(compKey);
    await redisClient
        .zadd(dataBlockHashKey(compKey), blockHeight, blockHash);
};

const cleanOlderData = redisClient => async (compKey, blockHeight) => {
    compKey = Buffer.from(compKey);
    const blockHashKey = dataBlockHashKey(compKey);
    const blockHashes = await redisClient.sendCommand('ZREVRANGEBYSCORE', [blockHashKey, blockHeight, '-inf']);
    for (let blockHash of blockHashes.slice(1)) {
        await redisClient.del(dataKey(compKey, blockHash));
        await redisClient.zrem(blockHashKey, blockHash);
    }
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

const closeRedis = () => new Promise((resolve, reject) => redisClient.quit(e => e ? reject(e) : resolve()));

module.exports = {
    // TODO: Rely on function name instead?
    getLatestBlockHeight: withRedisAndCache({ name: 'getLatestBlockHeight', cachedExpires: true }, getLatestBlockHeight),
    getLatestDataBlockHash: withRedisAndCache({ name: 'getLatestDataBlockHash' }, getLatestDataBlockHash),
    getData: withRedisAndCache({ name: 'getData' }, getData),
    scanDataKeys: withRedisAndCache({ name: 'scanDataKeys' }, scanDataKeys),
    setLatestBlockHeight: withRedis({ name: 'setLatestBlockHeight' }, setLatestBlockHeight),
    setData: withRedis({ name: 'setData' }, setData),
    deleteData: withRedis({ name: 'deleteData' }, deleteData),
    cleanOlderData: withRedis({ name: 'cleanOlderData' }, cleanOlderData),
    scanAllKeys: withRedis({ name: 'scanAllKeys' }, scanAllKeys),
    closeRedis,
}
