const { createClient } = require('redis');
const { promisify } = require("util");

const debug = require('debug')('storage');

let redisClient;

async function getRedisClient() {
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

async function getLatestBlockHeight() {
    debug('getLatestBlockHeight');
    try {
        const redisClient = await getRedisClient();
        return await redisClient.get('latest_block_height');
    } finally {
        debug('getLatestBlockHeight done');
    }
}

async function getLatestContractBlockHash(contractId, blockHeight) {
    debug('getLatestContractBlockHash', contractId, blockHeight);
    try {
        const redisClient = await getRedisClient();
        const [contractBlockHash] = await redisClient.sendCommand('ZREVRANGEBYSCORE',
            [Buffer.from(`code:${contractId}`), blockHeight, '-inf', 'LIMIT', '0', '1']);
        return contractBlockHash;
    } finally {
        debug('getLatestContractBlockHash done', contractId, blockHeight);
    }
}

async function getContractCode(contractId, blockHash) {
    debug('getContractCode', contractId);
    try {
        const redisClient = await getRedisClient();
        return await redisClient.get(Buffer.concat([Buffer.from(`code:${contractId}:`), blockHash]));
    } finally {
        debug('getContractCode done', contractId);
    }
}

async function getLatestDataBlockHash(redisKey, blockHeight) {
    redisKey = Buffer.from(redisKey);
    debug('getLatestDataBlockHash', redisKey.toString('utf8'), blockHeight);
    try {
        const redisClient = await getRedisClient();
        const [blockHash] = await redisClient.sendCommand('ZREVRANGEBYSCORE', [redisKey, blockHeight, '-inf', 'LIMIT', '0', '1']);
        return blockHash;
    } finally {
        debug('getLatestDataBlockHash done', redisKey.toString('utf8'), blockHeight);
    }
}

async function getData(redisKey, blockHash) {
    redisKey = Buffer.from(redisKey);
    debug('getData', redisKey.toString('utf8'));
    try {
        const redisClient = await getRedisClient();
        return await redisClient.get(Buffer.concat([redisKey, Buffer.from(':'), blockHash]));
    } finally {
        debug('getData done', redisKey.toString('utf8'));
    }
}

module.exports = {
    getLatestBlockHeight,
    getLatestContractBlockHash,
    getContractCode,
    getLatestDataBlockHash,
    getData,
};