// NOTE: Needs --experimental-wasm-bigint on older Node versions

const Koa = require('koa');
const app = new Koa();

const Router = require('koa-router');
const router = new Router();

const koaBody = require('koa-body')();

const WorkerPool = require('./worker-pool');

const contractCache = {};

let storageClient = require('./storage-client');
let workerPool;

async function runContract(contractId, methodName, methodArgs) {
    const debug = require('debug')(`host:${contractId}:${methodName}`);
    debug('runContract', contractId, methodName, methodArgs);

    if (!Buffer.isBuffer(methodArgs)) {
        methodArgs = Buffer.from(JSON.stringify(methodArgs));
    }

    if (!workerPool) {
        debug('workerPool');
        workerPool = new WorkerPool(10, storageClient);
        debug('workerPool done');
    }

    const latestBlockHeight = await storageClient.getLatestBlockHeight();
    debug('latestBlockHeight', latestBlockHeight)

    debug('find contract code')
    const contractBlockHash = await storageClient.getLatestContractBlockHash(contractId, latestBlockHeight);
    // TODO: Have cache based on code hash instead?
    const cacheKey = `${contractId}:${contractBlockHash.toString('hex')}}`;
    let wasmModule = contractCache[cacheKey];
    if (wasmModule) {
        debug('contract cache hit', cacheKey);
    } else {
        debug('contract cache miss', cacheKey);

        debug('blockHash', contractBlockHash);
        const wasmData = await storageClient.getContractCode(contractId, contractBlockHash);
        debug('wasmData.length', wasmData.length);

        debug('wasm compile');
        wasmModule = await WebAssembly.compile(wasmData);
        contractCache[cacheKey] = wasmModule;
        debug('wasm compile done');
    }

    debug('worker start');
    const result = await workerPool.runContract(latestBlockHeight, wasmModule, contractId, methodName, methodArgs);
    debug('worker done');
    return result;
}

// TODO: Extract tests
// (async function() {
//     console.time('everything')
//     const result = await runContract('dev-1629863402519-20649210409803', 'getChunk', {x: 0, y: 0});
//     await runContract('dev-1629863402519-20649210409803', 'web4_get', { request: { path: '/chunk/0,0' } });
//     await runContract('dev-1629863402519-20649210409803', 'web4_get', { request: { path: '/parcel/0,0' } });
//     // const result = await runContract('dev-1629863402519-20649210409803', 'web4_get', { request: { } });
//     console.log('runContract result', Buffer.from(result).toString('utf8'));
//     console.timeEnd('everything')
// })().catch(error => {
//     console.error(error);
//     process.exit(1);
// });

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
        const result = Buffer.from(await runContract(accountId, methodName, ctx.methodArgs));
        if (isJSON(result)) {
            ctx.type = 'json';
            ctx.body = result;
        }
    } catch (e) {
        const message = e.message;
        if (/TypeError.* is not a function/.test(message)) {
            ctx.throw(404, `method ${methodName} not found`);
        }

        ctx.throw(400, message);
    }
}

router.get('/account/:accountId/view/:methodName', parseQueryArgs, runViewMethod);
router.post('/account/:accountId/view/:methodName', koaBody, parseBodyArgs, runViewMethod);

app
    .use(async (ctx, next) => {
        console.log(ctx.method, ctx.path);
        await next();
    })
    .use(router.routes())
    .use(router.allowedMethods());

const PORT = process.env.PORT || 3000;
app.listen(PORT);
console.log('Listening on http://localhost:%d/', PORT);
