// TODO: Allow to choose storage type via env variable

const { DebugStorage } = require('./debug-wrapper');
const { RedisStorage } = require('./redis');
const { LMDBStorage } = require('./lmdb-embedded');

const storageType = process.env.FAST_NEAR_STORAGE_TYPE || 'redis';

module.exports = new DebugStorage(
    storageType === 'lmdb' ? new LMDBStorage() : new RedisStorage());