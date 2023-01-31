// TODO: Refactor common stuff with rest-api.test.js
const redis = require('./utils/redis');
redis.startIfNeeded();

const test = require('tape');
const { closeWorkerPool } = require('../run-contract');
test.onFinish(async () => {
    await closeWorkerPool();
    await redis.shutdown();
});

const { dumpChangesToRedis: handleStreamerMessage } = require('../scripts/load-from-near-lake');
const storage = require('../storage');
const app = require('../app');
const request = require('supertest')(app.callback());

const fs = require('fs');
const bs58 = require('bs58');
const TEST_CONTRACT_CODE = fs.readFileSync('test/data/test_contract_rs.wasm');

const sha256 = require('../utils/sha256');

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
                codeHash: bs58.encode(sha256(TEST_CONTRACT_CODE)),
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
            type: 'access_key_update',
            change: {
                accountId: 'test.near',
                publicKey: 'ed25519:JBHUrhF61wfScUxqGGRmfdJTQYg8MzRr5H8pqMMjqygr',
                accessKey: {
                    nonce: 1,
                    permission: {
                        FunctionCall: {
                            allowance: '246045981327662300000000',
                            methodNames: [],
                            receiverId: 'berry-or-not.near'
                        }
                    }
                }
            }
        }, {
            type: 'access_key_update',
            change: {
                accountId: 'test.near',
                publicKey: 'ed25519:GXHHscwTBRCBGRSjJc4nKZ4LKKnL2D5UDx5m78ps1KA4',
                accessKey: {
                    nonce: 123,
                    permission: 'FullAccess'
                }
            }
        }, {
            type: 'access_key_update',
            change: {
                accountId: 'test.near',
                publicKey: 'secp256k1:4dDYgTqZ7gdJHgq2HGfAkBPLHyAsrpjP56j6gWzAxoEwGZfwspqCCGWhhyTng9a1NbAnAu77v37bs15WLTSyZf6Q',
                accessKey: {
                    nonce: 7,
                    permission: 'FullAccess'
                }
            }
        }]
    }],
}

function testRequestImpl(testName, url, expectedStatus, expectedOutput, input, initFn) {
    test(testName, async t => {
        t.teardown(() => storage.clearDatabase());
        await initFn();

        let response;
        if (input) {
            response = await request
                .post(url)
                .responseType('blob')
                .send(input);
        } else {
            response = await request
                .get(url)
                .responseType('blob')
        }
        t.isEqual(response.status, expectedStatus);
        if (typeof expectedOutput === 'string') {
            t.isEqual(response.body.toString('utf8'), expectedOutput);
        } else if (Buffer.isBuffer(expectedOutput)) {
            t.isEquivalent(response.body, Buffer.from(expectedOutput));
        } else {
            t.isEqual(response.headers['content-type'], 'application/json; charset=utf-8');
            t.isEquivalent(JSON.parse(response.body.toString('utf8')), expectedOutput);
        }
    });
}

testRequestImpl('load data normally',
    '/account/test.near/view/fibonacci',
    200, Buffer.from([13, 0, 0, 0, 0, 0, 0, 0,]), Buffer.from([7]), async () => {
    await handleStreamerMessage(STREAMER_MESSAGE);
});

testRequestImpl('load data excluding test.near',
    '/account/test.near/view/fibonacci',
    404, 'accountNotFound: Account not found: test.near at 1 block height', Buffer.from([7]), async () => {
    await handleStreamerMessage(STREAMER_MESSAGE, { exclude: ['test.near'] });
});

testRequestImpl('load data including only lands.near',
    '/account/test.near/view/fibonacci',
    404, 'accountNotFound: Account not found: test.near at 1 block height', Buffer.from([7]), async () => {
    await handleStreamerMessage(STREAMER_MESSAGE, { include: ['lands.near'] });
});

testRequestImpl('load data including only test.near',
    '/account/test.near/view/fibonacci',
    200, Buffer.from([13, 0, 0, 0, 0, 0, 0, 0,]), Buffer.from([7]), async () => {
    await handleStreamerMessage(STREAMER_MESSAGE, { include: ['test.near'] });
});

testRequestImpl('include using glob pattern',
    '/account/test.near/view/fibonacci',
    404, 'accountNotFound: Account not found: test.near at 1 block height', Buffer.from([7]), async () => {
    await handleStreamerMessage(STREAMER_MESSAGE, { include: ['no-code.*'] });
});

testRequestImpl('exclude using glob pattern',
    '/account/test.near/view/fibonacci',
    404, 'accountNotFound: Account not found: test.near at 1 block height', Buffer.from([7]), async () => {
    await handleStreamerMessage(STREAMER_MESSAGE, { exclude: ['test.*'] });
});
