const { spawn } = require('child_process');

const TEST_REDIS_PORT = 7123;
const redisProcess = spawn('redis-server', ['--save', '', '--port', TEST_REDIS_PORT]);
process.env.FAST_NEAR_REDIS_URL = process.env.FAST_NEAR_REDIS_URL || `redis://localhost:${TEST_REDIS_PORT}`;

const test = require('tape');

const bs58 = require('bs58');
const { setLatestBlockHeight, setData, getData, closeRedis } = require('../storage-client');
const { accountKey } = require('../storage-keys');
const compressHistory = require('../scripts/compress-history');

test.onFinish(async () => {
    console.log('Killing Redis');
    redisProcess.kill();
    await closeRedis();
});

const TEST_ACCOUNT = 'test.near';
const BUF_123 = Buffer.from([1, 2, 3]);
const BUF_456 = Buffer.from([4, 5, 6]);

const BLOCKS = [
    { hash: bs58.decode('68dDfHtoaRwBM79uRWnQJ1eMSgehPW8JtnNRWkBpX87e'), index: 1, data: BUF_123 },
    { hash: bs58.decode('8dr6JVeZEhmW3Ls3mbuKbRp2TbNrr9hmC89n1ysJ1fC9'), index: 2, data: BUF_456 },
    { hash: bs58.decode('5y98LUcp7SBHLkmYM3mVSbqgfaPYxAFAMPLsi5wGedKv'), index: 3, data: BUF_123 },
];

test('single account, single entry', async t => {
    await setData(accountKey(TEST_ACCOUNT), BLOCKS[0].hash, BLOCKS[0].index, BLOCKS[0].data);
    await setLatestBlockHeight(BLOCKS[0].index);
    await compressHistory();

    const buf = await getData(accountKey(TEST_ACCOUNT), BLOCKS[0].hash);
    t.deepEqual(buf, BUF_123);
});

test('single account, multiple entry', async t => {
    for (let i = 0; i < BLOCKS.length; i++) {
        await setData(accountKey(TEST_ACCOUNT), BLOCKS[i].hash, BLOCKS[i].index, BLOCKS[i].data);
    }
    await setLatestBlockHeight(BLOCKS[2].index);
    await compressHistory();

    const buf = await getData(accountKey(TEST_ACCOUNT), BLOCKS[2].hash);
    t.deepEqual(buf, BUF_123);

    t.notOk(await getData(accountKey(TEST_ACCOUNT), BLOCKS[1].hash));
});



