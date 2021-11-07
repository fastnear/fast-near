// NOTE: Needs --experimental-wasm-bigint on older Node versions

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

(async function() {
    console.time('everything')
    const result = await runContract('dev-1629863402519-20649210409803', 'getChunk', {x: 0, y: 0});
    await runContract('dev-1629863402519-20649210409803', 'web4_get', { request: { path: '/chunk/0,0' } });
    await runContract('dev-1629863402519-20649210409803', 'web4_get', { request: { path: '/parcel/0,0' } });
    // const result = await runContract('dev-1629863402519-20649210409803', 'web4_get', { request: { } });
    console.log('runContract result', Buffer.from(result).toString('utf8'));
    console.timeEnd('everything')
})().catch(error => {
    console.error(error);
    process.exit(1);
});