const WorkerPool = require('./worker-pool');
const storageClient = require('./storage-client');
const resolveBlockHeight = require('./resolve-block-height');
const { FastNEARError } = require('./error');

const WORKER_COUNT = parseInt(process.env.FAST_NEAR_WORKER_COUNT || '4');

const LRU = require("lru-cache");
const { codeKey, accountKey } = require('./storage-keys');
let contractCache = new LRU({
    max: 25
});

let workerPool;

async function runContract(contractId, methodName, methodArgs, blockHeight) {
    const debug = require('debug')(`host:${contractId}:${methodName}`);
    debug('runContract', contractId, methodName, methodArgs, blockHeight);

    if (!Buffer.isBuffer(methodArgs)) {
        methodArgs = Buffer.from(JSON.stringify(methodArgs));
    }

    if (!workerPool) {
        debug('workerPool');
        workerPool = new WorkerPool(WORKER_COUNT, storageClient);
        debug('workerPool done');
    }

    const blockTimestamp = await storageClient.getBlockTimestamp(blockHeight);
    debug('blockTimestamp', blockTimestamp);

    debug('find contract code');
    const contractCodeKey = codeKey(contractId);
    const contractBlockHash = await storageClient.getLatestDataBlockHash(contractCodeKey, blockHeight);
    const accountDataKey = accountKey(contractId);

    const checkAccountExists = async () => {
        debug('load account data')
        const accountBlockHash = await storageClient.getLatestDataBlockHash(accountDataKey, blockHeight);
        debug('accountBlockHash', accountBlockHash);
        if (!accountBlockHash) {
            throw new FastNEARError('accountNotFound', `Account not found: ${contractId} at ${blockHeight} block height`);
        }

        const accountData = await storageClient.getData(accountDataKey, accountBlockHash);
        debug('accountData', accountData);
        if (!accountData) {
            throw new FastNEARError('accountNotFound', `Account not found: ${contractId} at ${blockHeight} block height`);
        }
    };

    if (!contractBlockHash) {
        await checkAccountExists();
        throw new FastNEARError('codeNotFound', `Cannot find contract code: ${contractId} ${blockHeight}`);
    }

    // TODO: Have cache based on code hash instead?
    const cacheKey = `${contractId}:${contractBlockHash.toString('hex')}}`;
    let wasmModule = contractCache.get(cacheKey);
    if (wasmModule) {
        debug('contract cache hit', cacheKey);
    } else {
        debug('contract cache miss', cacheKey);

        debug('blockHash', contractBlockHash);
        const wasmData = await storageClient.getData(contractCodeKey, contractBlockHash);
        if (!wasmData) {
            await checkAccountExists();
            throw new FastNEARError('codeNotFound', `Cannot find contract code: ${contractId} ${blockHeight}`);
        }
        debug('wasmData.length', wasmData.length);

        debug('wasm compile');
        wasmModule = await WebAssembly.compile(wasmData);
        contractCache.set(cacheKey, wasmModule);
        debug('wasm compile done');
    }

    debug('worker start');
    const { result, logs } = await workerPool.runContract(blockHeight, blockTimestamp, wasmModule, contractId, methodName, methodArgs);
    debug('worker done');
    return { result, logs, blockHeight, blockTimestamp };
}

async function closeWorkerPool() {
    if (workerPool) {
        await workerPool.close();
        workerPool = null;
    }
}

module.exports = {
    runContract,
    closeWorkerPool,
};

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