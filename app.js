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

async function runContract(contractId, methodName, args) {
    console.time('connect')
    const client = createClient();
    client.on('error', (err) => console.log('Redis Client Error', err));
    await client.connect();

    const latestBlockHeight = await client.get('latest_block_height');
    console.log('latestBlockHeight', latestBlockHeight)
    console.timeEnd('connect')

    console.time('load .wasm')
    const [contractBlockHash] = await client.sendCommand(['ZREVRANGEBYSCORE',
        `code:${contractId}`, latestBlockHeight, '-inf', 'LIMIT', '0', '1'], {}, true);

    const wasmData = await client.getBuffer(Buffer.concat([Buffer.from(`code:${contractId}:`), contractBlockHash]));
    console.log('wasmData', wasmData.length);
    console.timeEnd('load .wasm')

    console.time('wasm compile');
    const wasmModule = await WebAssembly.compile(wasmData);
    console.timeEnd('wasm compile');

    console.time('worker start');
    const result = await new Promise((resolve, reject) => {
        const worker = new Worker('./worker', {
            workerData: {
                wasmModule,
                contractId,
                methodName,
                args
            }
        });
        worker.on('online', () => console.timeEnd('worker start'));
        worker.on('message', message => {
            if (message.error) {
                return reject(message.error);
            }

            if (message.result) {
                return resolve(message.result);
            }
            
            switch (message.methodName) {
                case 'storage_read':
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
