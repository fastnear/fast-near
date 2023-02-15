const redis = require('./utils/redis');
redis.startIfNeeded();

const test = require('tape');

const bs58 = require('bs58');
const storage = require('../storage');
const { accountKey, ACCOUNT_SCOPE } = require('../storage-keys');
const compressHistory = require('../scripts/compress-history');

test.onFinish(async () => {
    await redis.shutdown();
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
    t.teardown(() => storage.clearDatabase());
    await storage.writeBatch(async batch => {
        await storage.setData(batch, ACCOUNT_SCOPE, TEST_ACCOUNT, null, BLOCKS[0].index, BLOCKS[0].data);
    });
    await storage.setLatestBlockHeight(BLOCKS[0].index);
    await compressHistory();

    const buf = await storage.getData(accountKey(TEST_ACCOUNT), BLOCKS[0].index);
    t.deepEqual(buf, BUF_123);
});

test('single account, multiple entry', async t => {
    t.teardown(() => storage.clearDatabase());
    await storage.writeBatch(async batch => {
        for (let i = 0; i < BLOCKS.length; i++) {
            await storage.setData(batch, ACCOUNT_SCOPE, TEST_ACCOUNT, null, BLOCKS[i].index, BLOCKS[i].data);
        }
    });
    await storage.setLatestBlockHeight(BLOCKS[2].index);
    await compressHistory();

    const buf = await storage.getData(accountKey(TEST_ACCOUNT), BLOCKS[2].index);
    t.deepEqual(buf, BUF_123);

    t.notOk(await storage.getData(accountKey(TEST_ACCOUNT), BLOCKS[1].index));
});



