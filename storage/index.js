// TODO: Allow to choose storage type via env variable

const { RedisStorage } = require('./redis');
const { LMDBStorage } = require('./lmdb-embedded');

const storageType = process.env.FAST_NEAR_STORAGE_TYPE || 'redis';

module.exports = storageType === 'lmdb' ? new LMDBStorage() : new RedisStorage();