// TODO: Allow to choose storage type via env variable

const { DebugStorage } = require('./debug-wrapper');
const { RedisStorage } = require('./redis');
//const { LMDBStorage } = require('./lmdb-embedded');
const { CachedStorage } = require('./cached');

const storageType = process.env.FAST_NEAR_STORAGE_TYPE || 'redis';

const ENABLE_CACHE = ['no', 'false', '0'].indexOf((process.env.FAST_NEAR_ENABLE_CACHE || 'true').trim().toLowerCase()) === -1;

const debugStorage = new DebugStorage(
//    storageType === 'lmdb'
//        ? new LMDBStorage()
//        :
    new RedisStorage());

module.exports =
    ENABLE_CACHE
        ? new CachedStorage(debugStorage)
        : debugStorage;
