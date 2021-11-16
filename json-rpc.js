const fetch = require('node-fetch');

const runContract  = require('./run-contract');
const storageClient = require('./storage-client');

class Account {
    amount;
    locked;
    code_hash;
    storage_usage;

    constructor(args) {
        Object.assign(this, args);
    }
}

const BORSH_SCHEMA = new Map([
    [Account, {
        kind: 'struct',
        fields: [
            ['amount', 'u128'],
            ['locked', 'u128'],
            ['code_hash', ['u8', 32]],
            ['storage_usage', 'u64'],
            // TODO: Make sure format is consistent for state dump and indexer
            // ['storage_paid_at_deprecated', 'u64']
        ]
    }]
]);

const { deserialize } = require('borsh');
const bs58 = require('bs58');
const debug = require('debug')('json-rpc');

// NOTE: This is JSON-RPC proxy needed to pretend we are actual nearcore
const NODE_URL = process.env.FAST_NEAR_NODE_URL || 'http://35.236.45.138:3030';

const proxyJson = async ctx => {
    debug('proxyJson', ctx.request.method, ctx.request.path, ctx.request.body);
    ctx.type = 'json';
    ctx.body = Buffer.from(await (await fetch(`${NODE_URL}${ctx.request.path}`, {
        method: ctx.request.method,
        headers: {
            'Content-Type': 'application/json'
        },
        body: ctx.request.body && JSON.stringify(ctx.request.body)
    })).arrayBuffer());
}

const handleJsonRpc = async ctx => {
    const { body } = ctx.request;
    if (body?.method == 'query' && body?.params?.request_type == 'call_function') {
        const { finality, account_id, method_name, args_base64 } = body.params;
        // TODO: Determine proper way to handle finality. Depending on what indexer can do maybe just redirect to nearcore if not final

        try {
            const { result, logs, blockHeight } = await runContract(account_id, method_name, Buffer.from(args_base64, 'base64'));
            const resultBuffer = Buffer.from(result);
            ctx.body = {
                jsonrpc: '2.0',
                result: {
                    result: Array.from(resultBuffer),
                    logs,
                    block_height: parseInt(blockHeight)
                    // TODO: block_hash
                },
                id: body.id
            };
            return;
        } catch (e) {
            // TODO: Proper error handling https://docs.near.org/docs/api/rpc/contracts#what-could-go-wrong-6
            const message = e.toString();
            if (/TypeError.* is not a function/.test(message)) {
                ctx.throw(404, `method ${methodName} not found`);
            }

            ctx.throw(400, message);
        }
    }

    if (body?.method == 'query' && body?.params?.request_type == 'view_account') {
        // TODO: Handle finality and block_id
        const { finality, block_id, account_id } = body.params;

        const latestBlockHeight = await storageClient.getLatestBlockHeight();
        debug('latestBlockHeight', latestBlockHeight);

        debug('find account data', account_id);
        const blockHash = await storageClient.getLatestAccountBlockHash(account_id, latestBlockHeight);
        debug('blockHash', blockHash);
        if (!blockHash) {
            // TODO: JSON-RPC error handling
            ctx.throw(404, `account ${account_id} not found`);
        }

        const accountData = await storageClient.getAccountData(account_id, blockHash);
        debug('account data loaded', account_id);
        if (!accountData) {
            // TODO: JSON-RPC error handling
            ctx.throw(404, `account ${account_id} not found`);
        }

        const { amount, locked, code_hash, storage_usage } = deserialize(BORSH_SCHEMA, Account, accountData);
        ctx.body = {
            jsonrpc: '2.0',
            result: {
                amount: amount.toString(),
                locked: locked.toString(),
                code_hash: bs58.encode(code_hash),
                storage_usage: parseInt(storage_usage.toString()),
                block_height: parseInt(latestBlockHeight)
                // TODO: block_hash
            },
            id: body.id
        };
        return;
    }

    await proxyJson(ctx);
};

module.exports = { handleJsonRpc, proxyJson };