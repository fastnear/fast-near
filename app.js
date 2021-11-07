// NOTE: Needs --experimental-wasm-bigint on older Node versions

const Koa = require('koa');
const app = new Koa();

const Router = require('koa-router');
const router = new Router();

const koaBody = require('koa-body')();

const {
    Worker
} = require('worker_threads');

const { createClient } = require('redis');

const contractCache = {};

async function runContract(contractId, methodName, args) {
    const debug = require('debug')(`host:${contractId}:${methodName}`);

    debug('connect')
    const client = createClient();
    client.on('error', (err) => console.error('Redis Client Error', err));
    await client.connect();

    const latestBlockHeight = await client.get('latest_block_height');
    debug('latestBlockHeight', latestBlockHeight)
    debug('connect done')

    debug('load .wasm')
    const [contractBlockHash] = await client.sendCommand(['ZREVRANGEBYSCORE',
        `code:${contractId}`, latestBlockHeight, '-inf', 'LIMIT', '0', '1'], {}, true);

    // TODO: Have cache based on code hash instead?
    const cacheKey = `${contractId}:${contractBlockHash.toString('hex')}}`;
    let wasmModule = contractCache[cacheKey];
    if (wasmModule) {
        debug('contract cache hit', cacheKey);
    } else {
        debug('contract cache miss', cacheKey);

        const wasmData = await client.getBuffer(Buffer.concat([Buffer.from(`code:${contractId}:`), contractBlockHash]));
        debug('wasmData.length', wasmData.length);
        debug('load .wasm done')

        debug('wasm compile');
        wasmModule = await WebAssembly.compile(wasmData);
        contractCache[cacheKey] = wasmModule;
        debug('wasm compile done');
    }

    debug('worker start');
    const result = await new Promise((resolve, reject) => {
        const worker = new Worker('./worker.js', {
            workerData: {
                wasmModule,
                contractId,
                methodName,
                args
            }
        });
        worker.on('online', () => debug('worker start done'));
        worker.on('message', message => {
            if (message.error) {
                return reject(message.error);
            }

            if (message.result) {
                return resolve(message.result);
            }
            
            switch (message.methodName) {
                case 'storage_read':
                    // TODO: Should be possible to coalesce parallel reads to the same key? Or will caching on HTTP level be enough?
                    const { redisKey } = message;
                    (async () => {
                        const [blockHash] = await client.sendCommand(['ZREVRANGEBYSCORE',
                            redisKey, latestBlockHeight, '-inf', 'LIMIT', '0', '1'], {}, true);

                        if (blockHash) {
                            const data = await client.getBuffer(Buffer.concat([redisKey, Buffer.from(':'), blockHash]));
                            worker.postMessage(data);
                        } else {
                            worker.postMessage(null);
                        }
                    })();
                    break;   
            }
        });
        worker.once('error', reject);
        worker.once('exit', (code) => {
            if (code !== 0) {
                reject(new Error(`Worker stopped with exit code ${code}`));
            }
        });
    });

    await client.disconnect();

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

// TODO: .get variant as web4 does?
router.post('/account/:accountId/view/:methodName', koaBody, async ctx => {
    const { accountId, methodName } = ctx.params;

    try {
        const result = Buffer.from(await runContract(accountId, methodName, ctx.request.body));
        if (isJSON(result)) {
            ctx.type = 'json';
            ctx.body = result;
        }
    } catch (e) {
        if (/TypeError.* is not a function/.test(e.toString())) {
            ctx.throw(404, `method ${methodName} not found`);
        }

        throw e;
    }
});

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
