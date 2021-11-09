const { createClient } = require('redis');
const { promisify } = require("util");

const debug = require('debug')('storage');

let redisClient;

function getRedisClient() {
    if (!redisClient) {
        redisClient = createClient({
            detect_buffers: true
        });
        redisClient.on('error', (err) => console.error('Redis Client Error', err));
    }

    return {
        get: promisify(redisClient.get).bind(redisClient),
        sendCommand: promisify(redisClient.sendCommand).bind(redisClient),
    };
}

const withRedis = (name, fn) => async (...args) => {
    debug(name, ...args);
    try {
        const redisClient = getRedisClient();   
        return await fn(redisClient)(...args);
    } finally {
        debug(`${name} done`, ...args);
    }
}

const getLatestBlockHeight = redisClient => async () => {
    return await redisClient.get('latest_block_height');
};

const getLatestContractBlockHash = redisClient => async (contractId, blockHeight) => {
    const [contractBlockHash] = await redisClient.sendCommand('ZREVRANGEBYSCORE',
        [Buffer.from(`code:${contractId}`), blockHeight, '-inf', 'LIMIT', '0', '1']);
    return contractBlockHash;
};

const getContractCode = redisClient => async (contractId, blockHash) => {
    return await redisClient.get(Buffer.concat([Buffer.from(`code:${contractId}:`), blockHash]));
};

const getLatestDataBlockHash = redisClient => async (redisKey, blockHeight) => {
    redisKey = Buffer.from(redisKey);
    const [blockHash] = await redisClient.sendCommand('ZREVRANGEBYSCORE',
        [redisKey, blockHeight, '-inf', 'LIMIT', '0', '1']);
    return blockHash;
};

const getData = redisClient => async (redisKey, blockHash) => {
    redisKey = Buffer.from(redisKey);
    return await redisClient.get(Buffer.concat([redisKey, Buffer.from(':'), blockHash]));
};

const exportsList = {
    getLatestBlockHeight,
    getLatestContractBlockHash,
    getContractCode,
    getLatestDataBlockHash,
    getData,
};

module.exports = Object.keys(exportsList)
    .map(key => ({ [key]: withRedis(key, exportsList[key]) }))
    .reduce((a, b) => Object.assign(a, b));