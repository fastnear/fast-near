const redis = require('./utils/redis');
redis.startIfNeeded();

const test = require('tape');
const { closeWorkerPool } = require('../run-contract');
test.onFinish(async () => {
    await closeWorkerPool();
    await redis.shutdown();
});

const { dumpChangesToRedis: handleStreamerMessage } = require('../scripts/load-from-near-lake');
const storage = require('../storage-client');
const app = require('../app');
const request = require('supertest')(app.callback());

const bs58 = require('bs58');
const crypto = require('crypto');
const fs = require('fs');
const TEST_CONTRACT_CODE = fs.readFileSync('test/data/test_contract_rs.wasm');
const LANDS_CONTRACT_CODE = fs.readFileSync('test/data/lands.near.wasm');

const LANDS_CHUNK_MODIFIED = {
    nonce: 0,
    tiles: [
        ['1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1', '-1'],
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

const LANDS_CHUNK_DEFAULT = {
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

const sha256 = (data) => {
    return crypto.createHash('sha256').update(data).digest();
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
                codeHash: bs58.encode(sha256(TEST_CONTRACT_CODE)),
                locked: '0',
                storageUsage: 20797,
            }
        }, {
            type: 'account_update',
            change: {
                accountId: 'lands.near',
                amount: '0',
                codeHash: bs58.encode(sha256(LANDS_CONTRACT_CODE)),
                locked: '0',
                storageUsage: 0,
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
        }, {
            type: 'data_update',
            change: {
                accountId: 'lands.near',
                keyBase64: Buffer.from('chunk:0:0').toString('base64'),
                valueBase64: Buffer.from(JSON.stringify(LANDS_CHUNK_MODIFIED)).toString('base64'),
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

const TEST_DELETION_STREAMER_MESSAGE = {
    block: {
        header: {
            height: 2,
            hash: 'DYY4cspC6cfMX29pZVokbB8avMQGcuFmKp1TKX4RH1M4',
            timestamp: Math.floor(Date.now() * 1000000) + 1000
        }
    },
    shards: [{
        stateChanges: [{
            type: 'account_deletion',
            change: {
                accountId: 'no-code.near',
            }
        }, {
            type: 'contract_code_deletion',
            change: {
                accountId: 'test.near',
            }
        }, {
            type: 'data_deletion',
            change: {
                accountId: 'lands.near',
                keyBase64: Buffer.from('chunk:0:0').toString('base64'),
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
        }]
    }],
}

test('/healthz (unsynced)', async t => {
    t.teardown(() => storage.clearDatabase());

    const response = await request.get('/healthz');
    t.isEqual(response.status, 500);
});

test('/healthz (synced)', async t => {
    t.teardown(() => storage.clearDatabase());

    await handleStreamerMessage(STREAMER_MESSAGE);

    const response = await request.get('/healthz');
    t.isEqual(response.status, 204);
});

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

function testRequest(testName, url, expectedStatus, expectedOutput, input = null) {
    testRequestImpl(testName, url, expectedStatus, expectedOutput, input, async () => {
        await handleStreamerMessage(STREAMER_MESSAGE);
    });
}

function testRequestAfterDeletion(testName, url, expectedStatus, expectedOutput, input = null) {
    testRequestImpl(`after deletion: ${testName}`, url, expectedStatus, expectedOutput, input, async () => {
        await handleStreamerMessage(STREAMER_MESSAGE);
        await handleStreamerMessage(TEST_DELETION_STREAMER_MESSAGE);
    });
}

function testRequestWithCompressHistory(testName, url, expectedStatus, expectedOutput, input = null) {
    testRequestImpl(`after compression: ${testName}`, url, expectedStatus, expectedOutput, input, async () => {
        await handleStreamerMessage(STREAMER_MESSAGE, { historyLength: 1 });
        await handleStreamerMessage(TEST_DELETION_STREAMER_MESSAGE, { historyLength: 1 });
    });
}

function testViewMethod(methodName, expectedStatus, expectedOutput, input = null) {
    const url = `/account/test.near/view/${methodName}`;
    testRequest(`call view method ${methodName}`, url, expectedStatus, expectedOutput, input);
}

testViewMethod('no-such-method', 404, 'method no-such-method not found');
testViewMethod('fibonacci', 200, Buffer.from([13, 0, 0, 0, 0, 0, 0, 0,]), Buffer.from([7]));
testViewMethod('ext_account_id', 200, 'test.near');
testViewMethod('ext_block_index', 200, Buffer.from([1, 0, 0, 0, 0, 0, 0, 0,]));
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

testRequest('call view method (no code)',
    '/account/no-code.near/view/someMethod', 404, 'codeNotFound: Cannot find contract code: no-code.near, block height: 1, code hash: 11111111111111111111111111111111');

testRequest('call view method with JSON in query args',
    '/account/lands.near/view/getChunk?x.json=0&y.json=0', 200, LANDS_CHUNK_MODIFIED);

testRequest('call view method with JSON in POST',
    '/account/lands.near/view/getChunk', 200, LANDS_CHUNK_MODIFIED, { x: 0, y: 0 });

testRequest('call view method with JSON in POST: missing key storage_read',
    '/account/lands.near/view/getChunk', 200, LANDS_CHUNK_DEFAULT, { x: 1, y: 1 });

testRequest('view account', '/account/test.near',
    200, {
        amount: '4936189930936415601114966690',
        locked: '0',
        code_hash: '6D7H4GEc5g7Pu7hwRLivKn7VETzKtqre7FZ6kWFr3sK7',
        storage_usage: 20797,
    });

testRequest('view account access key (function call)', '/account/test.near/key/ed25519:JBHUrhF61wfScUxqGGRmfdJTQYg8MzRr5H8pqMMjqygr',
    200, {
        public_key: 'ed25519:JBHUrhF61wfScUxqGGRmfdJTQYg8MzRr5H8pqMMjqygr',
        nonce: '1',
        type: 'FunctionCall',
        allowance: '246045981327662300000000',
        method_names: [],
        receiver_id: 'berry-or-not.near'
    });

testRequest('view account access key (full access)', '/account/test.near/key/ed25519:GXHHscwTBRCBGRSjJc4nKZ4LKKnL2D5UDx5m78ps1KA4',
    200, {
        public_key: 'ed25519:GXHHscwTBRCBGRSjJc4nKZ4LKKnL2D5UDx5m78ps1KA4',
        nonce: '123',
        type: 'FullAccess',
    });

testRequest('view contract data', '/account/test.near/data/*',
    200, {
        data: [
            [ '8charkey', 'test-value' ],
        ],
        iterator: '0',
    });

testRequest('download contract code',
    '/account/test.near/contract', 200, TEST_CONTRACT_CODE);

testRequest('list contract methods',
    '/account/test.near/contract/methods', 200, ['abort_with_zero', 'benchmark_storage_10kib', 'benchmark_storage_8b', 'call_promise', 'delete_strings', 'ext_account_balance', 'ext_account_id', 'ext_attached_deposit', 'ext_block_index', 'ext_block_timestamp', 'ext_predecessor_account_id', 'ext_prepaid_gas', 'ext_random_seed', 'ext_sha256', 'ext_signer_id', 'ext_signer_pk', 'ext_storage_usage', 'ext_used_gas', 'ext_validator_stake', 'ext_validator_total_stake', 'fibonacci', 'insert_strings', 'internal_recurse', 'log_something', 'log_u64', 'loop_forever', 'out_of_memory', 'panic_after_logging', 'panic_with_message', 'pass_through', 'read_value', 'recurse', 'run_test', 'run_test_with_storage_change', 'sum_n', 'sum_with_input', 'write_block_height', 'write_key_value', 'write_random_value']);

testRequestAfterDeletion('call view method (no such account)',
    '/account/no-code.near/view/someMethod', 404,'accountNotFound: Account not found: no-code.near at 2 block height');

testRequestAfterDeletion('call view method (no code)',
    '/account/test.near/view/ext_account_id', 404, 'codeNotFound: Cannot find contract code: test.near, block height: 2, code hash: 11111111111111111111111111111111');

testRequestAfterDeletion('call view method with JSON in POST',
    '/account/lands.near/view/getChunk', 200, LANDS_CHUNK_DEFAULT, { x: 0, y: 0 });

// NOTE: After history compression should match regular tests, but at block index 2
testRequestWithCompressHistory('call view method (no such account)',
    '/account/no-code.near/view/someMethod', 404,'accountNotFound: Account not found: no-code.near at 2 block height');

testRequestWithCompressHistory('call view method (no code)',
    '/account/test.near/view/ext_account_id', 404, 'codeNotFound: Cannot find contract code: test.near, block height: 2, code hash: 11111111111111111111111111111111');

testRequestWithCompressHistory('call view method with JSON in POST',
    '/account/lands.near/view/getChunk', 200, LANDS_CHUNK_DEFAULT, { x: 0, y: 0 });