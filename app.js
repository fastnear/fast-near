// NOTE: Needs --experimental-wasm-bigint on older Node versions

const Koa = require('koa');
const app = new Koa();
const Router = require('koa-router');
const router = new Router();
const koaBody = require('koa-body')();

const fetch = require('node-fetch');

const runContract = require('./run-contract');

function isJSON(buffer) {
    try {
        const MAX_WHITESPACE = 1000;
        const startSlice = buffer.slice(0, MAX_WHITESPACE + 1).toString('utf8').trim();
        if (startSlice.startsWith('[') || startSlice.startsWith('[')) {
            JSON.parse(buffer.toString('utf8'));
        }
    } catch (e) {
        // Ignore error, means it's not valid JSON
        return false;
    }

    return true;
}

const parseQueryArgs = async (ctx, next) => {
    // TODO: Refactor/merge with web4?
    const {
        query
    } = ctx;

    const tryParse = key => {
        try {
            return JSON.parse(query[key]);
        } catch (e) {
            ctx.throw(400, `Problem parsing JSON for ${key} field: ${e}`);
        }
    }

    ctx.methodArgs = Object.keys(query)
        .map(key => key.endsWith('.json')
            ? { [key.replace(/\.json$/, '')]: tryParse(key) }
            : { [key] : query[key] })
        .reduce((a, b) => ({...a, ...b}), {});

    await next();
}

const parseBodyArgs = async (ctx, next) => {
    ctx.methodArgs = ctx.request.body;

    await next();
}

const runViewMethod = async ctx => {
    const { accountId, methodName } = ctx.params;

    try {
        const { result, logs } = await runContract(accountId, methodName, ctx.methodArgs);
        // TODO: return logs somehow (in headers? if requested?)
        const resultBuffer = Buffer.from(result);
        if (isJSON(resultBuffer)) {
            ctx.type = 'json';
            ctx.body = resultBuffer;
        }
    } catch (e) {
        const message = e.toString();
        if (/TypeError.* is not a function/.test(message)) {
            ctx.throw(404, `method ${methodName} not found`);
        }

        ctx.throw(400, message);
    }
}

router.get('/account/:accountId/view/:methodName', parseQueryArgs, runViewMethod);
router.post('/account/:accountId/view/:methodName', koaBody, parseBodyArgs, runViewMethod);

const MAX_LIMIT = 100;
router.get('/account/:accountId/data/:keyPattern', async ctx => {
    const { encoding = 'utf8', iterator = "0", limit = "10"} = ctx.query;
    const { accountId, keyPattern } = ctx.params;
    const debug = require('debug')(`data:${accountId}`);

    const latestBlockHeight = await storageClient.getLatestBlockHeight();
    debug('latestBlockHeight', latestBlockHeight);

    const { data, iterator: newIterator } = await storageClient.scanDataKeys(accountId, latestBlockHeight, keyPattern, iterator, Math.min(MAX_LIMIT, parseInt(limit)));
    ctx.body = {
        data: data
            .filter(([_, value]) => value !== null)
            .map(([key, value]) => [Buffer.from(key).toString(encoding), Buffer.from(value).toString(encoding)]),
        iterator: newIterator
    };
});

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
            ['storage_paid_at_deprecated', 'u64']
        ]
    }]
]);

const { deserialize } = require('borsh');
const bs58 = require('bs58');

// NOTE: This is JSON-RPC proxy needed to pretend we are actual nearcore
const NODE_URL = process.env.FAST_NEAR_NODE_URL || 'http://35.236.45.138:3030';

const proxyJson = async ctx => {
    ctx.type = 'json';
    ctx.body = Buffer.from(await (await fetch(`${NODE_URL}${ctx.request.path}`, {
        method: ctx.request.method,
        headers: {
            'Content-Type': 'application/json'
        },
        body: ctx.request.body && JSON.stringify(ctx.request.body)
    })).arrayBuffer());
}

router.post('/', koaBody, async ctx => {
    const debug = require('debug')('json-rpc');

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
    }

    await proxyJson(ctx);
});

router.get('/(status|metrics|health)', proxyJson);

app
    .use(async (ctx, next) => {
        console.log(ctx.method, ctx.path);
        await next();
    })
    .use(router.routes())
    .use(router.allowedMethods());

const HOST = process.env.HOST|| '0.0.0.0';
const PORT = process.env.PORT || 3000;
app.listen(PORT, HOST);
console.log('Listening on http://%s:%d/', HOST, PORT);
