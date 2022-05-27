// TODO: Refactor Redis stuff with other tests
const { spawn } = require('child_process');

const TEST_REDIS_PORT = 7123;
const redisProcess = spawn('redis-server', ['--save', '', '--port', TEST_REDIS_PORT]);
process.env.FAST_NEAR_REDIS_URL = process.env.FAST_NEAR_REDIS_URL || `redis://localhost:${TEST_REDIS_PORT}`;

const test = require('tape');

const bs58 = require('bs58');
const { setLatestBlockHeight, setData, getData, closeRedis, redisBatch } = require('../storage-client');
const { accountKey } = require('../storage-keys');
const { closeWorkerPool } = require('../run-contract');

test.onFinish(async () => {
    await closeWorkerPool();
    console.log('Killing Redis');
    redisProcess.kill();
    await closeRedis();
});

const { handleStreamerMessage } = require('../scripts/load-from-near-lake');
const app = require('../app');
const request = require('supertest')(app.callback());

const fs = require('fs');

const STREAMER_MESSAGE = {
    block: {
        header: {
            height: 1,
            hash: '68dDfHtoaRwBM79uRWnQJ1eMSgehPW8JtnNRWkBpX87e',
            timestamp: Math.floor(Date.now() * 1000000)
        }
    },
    shards: [{
        stateChanges: [{
            type: 'contract_code_update',
            change: {
                accountId: 'test.near',
                codeBase64: fs.readFileSync('test/data/test_contract_rs.wasm').toString('base64'),
            }
        }]
    }],
}

test('/healthz (unsynced)', async t => {
    const response = await request.get('/healthz');
    t.isEqual(response.status, 500);
});

test('/healthz (synced)', async t => {
    await handleStreamerMessage(STREAMER_MESSAGE);
    const response = await request.get('/healthz');
    t.isEqual(response.status, 204);
});

test('call view method (no such method)', async t => {
    await handleStreamerMessage(STREAMER_MESSAGE);
    const response = await request.get('/account/test.near/view/no-such-method');
    t.isEqual(response.status, 404);
});

function testViewMethodSuccess(methodName, expectedOutput, input = null) {
    test(`call view method ${methodName}`, async t => {
        const url = `/account/test.near/view/${methodName}`;
        let response;
        if (input) {
            response = await request
                .post(url)
                .responseType('blob')
                .send(Buffer.from(input));
        } else {
            response = await request
                .get(url)
                .responseType('blob')
        }
        t.isEqual(response.status, 200);
        t.isEquivalent(response.body, Buffer.from(expectedOutput));
    });
}

testViewMethodSuccess('fibonacci', [13, 0, 0, 0, 0, 0, 0, 0,], [7]);
testViewMethodSuccess('ext_account_id', 'test.near');
testViewMethodSuccess('ext_block_index', [1, 0, 0, 0, 0, 0, 0, 0,]);