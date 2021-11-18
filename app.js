// NOTE: Needs --experimental-wasm-bigint on older Node versions

const Koa = require('koa');
const app = new Koa();
const Router = require('koa-router');
const router = new Router();
const getRawBody = require('raw-body');
const cors = require('@koa/cors');

const runContract = require('./run-contract');
const storageClient = require('./storage-client');

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
router.post('/account/:accountId/view/:methodName', parseBodyArgs, runViewMethod);

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

const HOST = process.env.HOST|| '0.0.0.0';
const PORT = process.env.PORT || 3000;
app.listen(PORT, HOST);
console.log('Listening on http://%s:%d/', HOST, PORT);
