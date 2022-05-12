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
        zadd: promisify(redisClient.zadd).bind(redisClient),
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
    return await redisClient.get('latest_block_height');
};

const setLatestBlockHeight = redisClient => async (blockHeight) => {
    console.log('redisClient', redisClient);
    return await redisClient.set('latest_block_height', blockHeight.toString());
}

const getLatestContractBlockHash = redisClient => async (contractId, blockHeight) => {
    const [contractBlockHash] = await redisClient.sendCommand('ZREVRANGEBYSCORE',
        [Buffer.from(`code:${contractId}`), blockHeight, '-inf', 'LIMIT', '0', '1']);
    return contractBlockHash;
};

const getContractCode = redisClient => async (contractId, blockHash) => {
    return await redisClient.get(Buffer.concat([Buffer.from(`code:${contractId}:`), blockHash]));
};

function accountBlockHashKey(accountId) {
    return Buffer.from(`account:${accountId}`);
}

function accountDataKey(accountId, blockHash) {
    return Buffer.concat([Buffer.from(`account-data:${accountId}:`), blockHash]);
}

const getLatestAccountBlockHash = redisClient => async (accountId, blockHeight) => {
    const [blockHash] = await redisClient.sendCommand('ZREVRANGEBYSCORE',
        [accountBlockHashKey(accountId), blockHeight, '-inf', 'LIMIT', '0', '1']);
    return blockHash;
};

const getAccountData = redisClient => async (accountId, blockHash) => {
    return await redisClient.get(accountDataKey(accountId, blockHash));
};

const setAccountData = redisClient => async (accountId, blockHash, blockHeight, data) => {
    await redisClient
        .set(accountDataKey(accountId, blockHash), data);
    await redisClient
        .zadd(accountBlockHashKey(accountId), blockHeight, blockHash);
};

function dataBlockHashKey(compKey) {
    return Buffer.concat([Buffer.from('data:'), compKey]);
}

function dataKey(compKey, blockHash) {
    return Buffer.concat([Buffer.from('data-value:'), compKey, Buffer.from(':'), blockHash]);
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

const exportsMap = {
    getLatestBlockHeight,
    getLatestContractBlockHash,
    getContractCode,
    getLatestAccountBlockHash,
    getAccountData,
    getLatestDataBlockHash,
    getData,
    scanDataKeys,
};

const cacheExpiresList = [ getLatestBlockHeight ];

const readMethods = Object.keys(exportsMap)
    .map(name => ({ [name]: withRedisAndCache({ name, cachedExpires: cacheExpiresList.includes(exportsMap[name]) }, exportsMap[name]) }))
    .reduce((a, b) => Object.assign(a, b));

const writeExportsMap = {
    setLatestBlockHeight,
    setAccountData,
    setData,
}

const writeMethods = Object.keys(writeExportsMap)
    .map(name => ({ [name]: withRedis({ name }, writeExportsMap[name]) }))
    .reduce((a, b) => Object.assign(a, b));

module.exports = {
    ...readMethods,
    ...writeMethods
}

