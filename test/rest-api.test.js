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
                codeBase64: fs.readFileSync('test/data/test_contract_rs.wasm').toString('base64'),
            }
        }, {
            type: 'data_update',
            change: {
                accountId: 'test.near',
                keyBase64: Buffer.from('8charkey').toString('base64'),
                valueBase64: Buffer.from('test-value').toString('base64'),
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

function testViewMethod(methodName, expectedStatus, expectedOutput, input = null) {
    test(`call view method ${methodName}`, async t => {
        t.teardown(clearDatabase);
        await handleStreamerMessage(STREAMER_MESSAGE);

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
        t.isEqual(response.status, expectedStatus);
        if (typeof expectedOutput === 'string') {
            t.isEqual(response.body.toString('utf8'), expectedOutput);
        } else {
            t.isEquivalent(response.body, Buffer.from(expectedOutput));
        }
    });
}

testViewMethod('no-such-method', 404, 'method no-such-method not found');
testViewMethod('fibonacci', 200, [13, 0, 0, 0, 0, 0, 0, 0,], [7]);
testViewMethod('ext_account_id', 200, 'test.near');
testViewMethod('ext_block_index', 200, [1, 0, 0, 0, 0, 0, 0, 0,]);
testViewMethod('read_value', 200, 'test-val', '8charkey');
// TODO: Propagate logs somehow?
testViewMethod('log_something', 200, '');
testViewMethod('loop_forever', 400, 'Error: test.near.loop_forever execution timed out');
testViewMethod('abort_with_zero', 400, 'Error: String encoding is bad UTF-16 sequence.');
testViewMethod('panic_with_message', 400, 'Error: WAT?');
// TODO: Propagate logs somehow?
testViewMethod('panic_after_logging', 400, 'Error: WAT?');

test('view account state', async t => {
    t.teardown(clearDatabase);
    await handleStreamerMessage(STREAMER_MESSAGE);

    const response = await request.get('/account/test.near/state');
    t.isEqual(response.status, 200);
    t.isEquivalent(response.body, {
        amount: '4936189930936415601114966690',
        codeHash: '11111111111111111111111111111111',
        locked: '0',
        storageUsage: 20797,
    });
});