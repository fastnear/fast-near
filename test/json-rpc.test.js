const test = require('tape');
const app = require('../app');

const request = require('supertest')(app.callback());

const { closeWorkerPool } = require('../run-contract');
test.onFinish(async () => {
    await closeWorkerPool();
});

test('can retrieve block chunk', async t => {
    const res = await request.post('/')
        .send({ 
            jsonrpc: '2.0',
            method: 'chunk',
            id: 'whatever',
            params: { block_id: 122061804, shard_id: 0 }
    });

    t.isEqual(res.status, 200);
    t.isEqual(res.body.jsonrpc, '2.0');
    t.isEqual(res.body.id, 'whatever');
    t.isEqual(res.body.result.author, 'epic.poolv1.near');
    t.deepEqual(res.body.result.header, {
        balance_burnt: '3966547313917200000000',
        chunk_hash: '5nDcdmLFCXbGEhvvNVvQzhJqd3fpJe7884SdFWvSpgY8',
        encoded_length: 39243,
        encoded_merkle_root: 'EkRyMEBWcqPCj5X4yvEr1tqBDBemVEP219U1Ln4GYey7',
        gas_limit: 1000000000000000,
        gas_used: 67321303793185,
        height_created: 122061804,
        height_included: 122061804,
        outcome_root: 'CvPriVSrLJDWeMs4irNJMg3TMJjTYwgfHHCz4Sc5qWX7',
        outgoing_receipts_root: '4ebLtQKz4dr5dAatGwQ7BWDd3oiFmuLJ8k2HDYz6drhK',
        prev_block_hash: 'GwzeQP6mTb3oqmyA5d1uDTwB6z7EHPZZUzmbdGXvRVJk',
        prev_state_root: 'BjASXABPBgug39z54kye3iJ2oaChcuF5xiheVbrSUhgq',
        rent_paid: '0',
        shard_id: 0,
        signature: 'ed25519:2c6m4hBiPrspAeGN85XQpPTucDC2pcMvHb86eiPuTS9oPLWyXWDswfnSmPqT7jff8wntWbVdvvdQM5gd9FkgGdpn',
        tx_root: 'GP1jZJAgFytPpY11ypAfKsHagWKXqx53DC8j6fViGt4U',
        validator_proposals: [],
        validator_reward: '0'
    });
});

test('chunk not found', async t => {
    const res = await request.post('/')
        .send({ 
            jsonrpc: '2.0',
            method: 'chunk',
            id: 'whatever',
            params: { block_id: 999999, shard_id: 0 }
    });

    t.isEqual(res.status, 200);
    t.isEqual(res.body.id, 'whatever');
    t.deepEqual(res.body.error, {
        name: 'HANDLER_ERROR',
        cause: { info: {}, name: 'UNKNOWN_BLOCK' },
        code: -32000,
        data: 'DB Not Found Error: BLOCK HEIGHT: 999999 \n Cause: Unknown',
        message: 'Server error'
    });

});