const redis = require('./utils/redis');
redis.startIfNeeded();

const test = require('tape');
const { closeWorkerPool } = require('../run-contract');
test.onFinish(async () => {
    await closeWorkerPool();
    await redis.shutdown();
});

const { handleStreamerMessage } = require('../scripts/load-from-near-lake');
const { clearDatabase } = require('../storage-client');
const app = require('../app');
const request = require('supertest')(app.callback());

const fs = require('fs');
const TEST_CONTRACT_CODE = fs.readFileSync('test/data/test_contract_rs.wasm');
const LANDS_CONTRACT_CODE = fs.readFileSync('test/data/lands.near.wasm');

const LANDS_CHUNK = {
    nonce: 0,
    tiles: [
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
        ['-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1']
    ]
};

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
            type: 'account_update',
            change: {
                accountId: 'no-code.near',
                amount: '4936189930936415601114966690',
                codeHash: '11111111111111111111111111111111',
                locked: '0',
                storageUsage: 20797,
            }
        }, {
            type: 'account_update',
            change: {
                accountId: 'test.near',
                amount: '4936189930936415601114966690',
                codeHash: '11111111111111111111111111111111',
                locked: '0',
                storageUsage: 20797,
            }
        }, {
            type: 'contract_code_update',
            change: {
                accountId: 'test.near',
                codeBase64: TEST_CONTRACT_CODE.toString('base64'),
            }
        }, {
            type: 'data_update',
            change: {
                accountId: 'test.near',
                keyBase64: Buffer.from('8charkey').toString('base64'),
                valueBase64: Buffer.from('test-value').toString('base64'),
            }
        }, {
            type: 'contract_code_update',
            change: {
                accountId: 'lands.near',
                codeBase64: LANDS_CONTRACT_CODE.toString('base64'),
            }
        }]
    }],
}

test('/healthz (unsynced)', async t => {
    t.teardown(clearDatabase);

    const response = await request.get('/healthz');
    t.isEqual(response.status, 500);
});

test('/healthz (synced)', async t => {
    t.teardown(clearDatabase);
    await handleStreamerMessage(STREAMER_MESSAGE);

    const response = await request.get('/healthz');
    t.isEqual(response.status, 204);
});

const isObject = obj => obj !== null && !Array.isArray(obj) && typeof obj === 'object';

function testRequest(testName, url, expectedStatus, expectedOutput, input = null) {
    test(testName, async t => {
        t.teardown(clearDatabase);
        await handleStreamerMessage(STREAMER_MESSAGE);

        let response;
        if (input) {
            response = await request
                .post(url)
                .responseType('blob')
                .send(isObject(input) ? input : Buffer.from(input));
        } else {
            response = await request
                .get(url)
                .responseType('blob')
        }
        t.isEqual(response.status, expectedStatus);
        if (typeof expectedOutput === 'string') {
            t.isEqual(response.body.toString('utf8'), expectedOutput);
        } if (isObject(expectedOutput) && !Buffer.isBuffer(expectedOutput)) {
            t.isEqual(response.headers['content-type'], 'application/json; charset=utf-8');
            t.isEquivalent(JSON.parse(response.body.toString('utf8')), expectedOutput);
        } else {
            t.isEquivalent(response.body, Buffer.from(expectedOutput));
        }
    });
}

function testViewMethod(methodName, expectedStatus, expectedOutput, input = null) {
    const url = `/account/test.near/view/${methodName}`;
    testRequest(`call view method ${methodName}`, url, expectedStatus, expectedOutput, input);
}

testViewMethod('no-such-method', 404, 'method no-such-method not found');
testViewMethod('fibonacci', 200, [13, 0, 0, 0, 0, 0, 0, 0,], [7]);
testViewMethod('ext_account_id', 200, 'test.near');
testViewMethod('ext_block_index', 200, [1, 0, 0, 0, 0, 0, 0, 0,]);
testViewMethod('read_value', 200, 'test-val', '8charkey');
// TODO: Propagate logs somehow?
testViewMethod('log_something', 200, '');
testViewMethod('loop_forever', 400, 'executionTimedOut: test.near.loop_forever execution timed out');
testViewMethod('abort_with_zero', 400, 'abort: String encoding is bad UTF-16 sequence.');
testViewMethod('panic_with_message', 400, 'panic: WAT?');
// TODO: Propagate logs somehow?
testViewMethod('panic_after_logging', 400, 'panic: WAT?');

testRequest('call view method (no such account)',
    '/account/no-such-account.near/view/someMethod', 404,'accountNotFound: Account not found: no-such-account.near at 1 block height');

testRequest('call view method (no such account)',
    '/account/no-code.near/view/someMethod', 404, 'codeNotFound: Cannot find contract code: no-code.near 1');

testRequest('call view method with JSON in query args',
    '/account/lands.near/view/getChunk?x.json=0&y.json=0', 200, LANDS_CHUNK);

testRequest('call view method with JSON in POST',
    '/account/lands.near/view/getChunk', 200, LANDS_CHUNK, { x: 0, y: 0 });

testRequest('view account state', '/account/test.near/state',
    200, {
        amount: '4936189930936415601114966690',
        codeHash: '11111111111111111111111111111111',
        locked: '0',
        storageUsage: 20797,
    });

testRequest('download contract code',
    '/account/test.near/contract', 200, TEST_CONTRACT_CODE);