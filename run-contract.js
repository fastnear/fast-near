const WorkerPool = require('./worker-pool');
const storageClient = require('./storage-client');

const WORKER_COUNT = parseInt(process.env.FAST_NEAR_WORKER_COUNT || '4');

const contractCache = {};

let workerPool;

async function runContract(contractId, methodName, methodArgs) {
    const debug = require('debug')(`host:${contractId}:${methodName}`);
    debug('runContract', contractId, methodName, methodArgs);

    if (!Buffer.isBuffer(methodArgs)) {
        methodArgs = Buffer.from(JSON.stringify(methodArgs));
    }

    if (!workerPool) {
        debug('workerPool');
        workerPool = new WorkerPool(WORKER_COUNT, storageClient);
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
    const { result, logs } = await workerPool.runContract(latestBlockHeight, wasmModule, contractId, methodName, methodArgs);
    debug('worker done');
    return { result, logs, blockHeight: latestBlockHeight };
}

module.exports = runContract;

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