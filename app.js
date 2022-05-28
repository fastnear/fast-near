// NOTE: Needs --experimental-wasm-bigint on older Node versions

const Koa = require('koa');
const app = new Koa();
const Router = require('koa-router');
const router = new Router();
const getRawBody = require('raw-body');
const cors = require('@koa/cors');

const resolveBlockHeightUtil = require('./resolve-block-height');
const { runContract } = require('./run-contract');
const storageClient = require('./storage-client');
const { codeKey, accountKey } = require('./storage-keys');
const { deserialize } = require('borsh');
const bs58 = require('bs58');
const { BORSH_SCHEMA, Account } = require('./data-model');

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
    ctx.methodArgs = await getRawBody(ctx.req);

    await next();
}

const resolveBlockHeight = async (ctx, next) => {
    // TODO: Evaluate some alternate scheme, e.g. use URL prefix like /height/<block_height>/account/
    const { near_block_height: blockHeight } = ctx.query;

    ctx.blockHeight = await resolveBlockHeightUtil(blockHeight);

    await next();
}

const runViewMethod = async ctx => {
    const { accountId, methodName } = ctx.params;
    const { blockHeight } = ctx;

    try {
        const { result, logs } = await runContract(accountId, methodName, ctx.methodArgs, blockHeight);
        // TODO: return logs somehow (in headers? if requested?)
        const resultBuffer = Buffer.from(result);
        if (isJSON(resultBuffer)) {
            ctx.type = 'json';
            ctx.body = resultBuffer;
        }
    } catch (e) {
        console.error('e', e);
        const message = e.toString();
        if (/TypeError.* is not a function/.test(message)) {
            ctx.throw(404, `method ${methodName} not found`);
        }

        ctx.throw(400, message);
    }
}

router.get('/account/:accountId/view/:methodName', resolveBlockHeight, parseQueryArgs, runViewMethod);
router.post('/account/:accountId/view/:methodName', resolveBlockHeight, parseBodyArgs, runViewMethod);

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

router.get('/account/:accountId/state', resolveBlockHeight, async ctx => {
    const { accountId } = ctx.params;

    // TODO: Refactor with JSON-RPC version?
    const blockHash = await storageClient.getLatestDataBlockHash(accountKey(accountId), ctx.blockHeight);
    if (!blockHash) {
        ctx.throw(404);
    }

    const data = await storageClient.getData(accountKey(accountId), blockHash);
    if (!data) {
        ctx.throw(404);
    }

    const { amount, locked, code_hash, storage_usage } = deserialize(BORSH_SCHEMA, Account, data);
    ctx.body = {
        amount: amount.toString(),
        locked: locked.toString(),
        codeHash: bs58.encode(code_hash),
        storageUsage: parseInt(storage_usage.toString()),
    };
});

router.get('/account/:accountId/contract', resolveBlockHeight, async ctx => {
    const { accountId } = ctx.params;

    const blockHash = await storageClient.getLatestDataBlockHash(codeKey(accountId), ctx.blockHeight);
    if (!blockHash) {
        ctx.throw(404);
    }

    const data = await storageClient.getData(codeKey(accountId), blockHash);
    if (!data) {
        ctx.throw(404);
    }

    ctx.type = 'wasm';
    ctx.res.setHeader('Content-Disposition', `attachment; filename="${accountId}.wasm"`);
    ctx.body = data;
});

const MAX_BLOCK_LAG_TIME_MS = 20000;
router.get('/healthz', async ctx => {
    const latestBlockHeight = await storageClient.getLatestBlockHeight();
    const latestBlockTimestamp = (await storageClient.getBlockTimestamp(latestBlockHeight)) / 1000000;

    // NOTE: fast-near node considered unhealthy if it's out of sync
    if (Date.now() - latestBlockTimestamp > MAX_BLOCK_LAG_TIME_MS) {
        ctx.throw(500, 'unhealthy (out of sync)');
    }

    ctx.status = 204; // No Content
});

const { proxyJson, handleJsonRpc } = require('./json-rpc');
router.post('/', handleJsonRpc);
router.get('/(status|metrics|health)', proxyJson);

app
    .use(async (ctx, next) => {
        console.log(ctx.method, ctx.path);
        await next();
    })
    .use(cors({ credentials: true }))
    .use(router.routes())
    .use(router.allowedMethods());

module.exports = app;

if (require.main === module) {
    const HOST = process.env.HOST|| '0.0.0.0';
    const PORT = process.env.PORT || 3000;
    app.listen(PORT, HOST);
    console.log('Listening on http://%s:%d/', HOST, PORT);
}
