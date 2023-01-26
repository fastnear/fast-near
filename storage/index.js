// TODO: Allow to choose storage type via env variable

const { RedisStorage } = require('./redis');
const { LMDBStorage } = require('./lmdb-embedded');

module.exports = new LMDBStorage();