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
    const redisClient = await getRedisClient();   
    return await redisClient.get('latest_block_height');
}

async function getLatestContractBlockHash(contractId, blockHeight) {
    const redisClient = await getRedisClient();   
    const [contractBlockHash] = await redisClient.sendCommand('ZREVRANGEBYSCORE',
        [Buffer.from(`code:${contractId}`), blockHeight, '-inf', 'LIMIT', '0', '1']);
    return contractBlockHash;
}

async function getContractCode(contractId, blockHash) {
    const redisClient = await getRedisClient();   
    return await redisClient.get(Buffer.concat([Buffer.from(`code:${contractId}:`), blockHash]));
}

async function getLatestDataBlockHash(redisKey, blockHeight) {
    const redisClient = await getRedisClient();   
    const [blockHash] = await redisClient.sendCommand('ZREVRANGEBYSCORE', [Buffer.from(redisKey), blockHeight, '-inf', 'LIMIT', '0', '1']);
    return blockHash;
}

async function getData(redisKey, blockHash) {
    const redisClient = await getRedisClient();   
    return await redisClient.get(Buffer.concat([Buffer.from(redisKey), Buffer.from(':'), blockHash]));
}

module.exports = {
    getLatestBlockHeight,
    getLatestContractBlockHash,
    getContractCode,
    getLatestDataBlockHash,
    getData,
};