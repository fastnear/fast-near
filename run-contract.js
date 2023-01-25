const WorkerPool = require('./worker-pool');
const storageClient = require('./storage-client');
const { FastNEARError } = require('./error');

const WORKER_COUNT = parseInt(process.env.FAST_NEAR_WORKER_COUNT || '4');

const LRU = require("lru-cache");
const { codeKey, accountKey } = require('./storage-keys');
let contractCache = new LRU({
    max: 25
});

let workerPool;

async function getWasmModule(contractId, blockHeight) {
    const debug = require('debug')(`host:${contractId}`);
    debug('getWasmModule', contractId, blockHeight);

    debug('find contract code');
    const contractCodeKey = codeKey(contractId);
    const accountDataKey = accountKey(contractId);

    const checkAccountExists = async () => {
        debug('load account data')
        const accountData = await storageClient.getLatestData(accountDataKey, blockHeight);
        debug('accountData', accountData);
        if (!accountData) {
            throw new FastNEARError('accountNotFound', `Account not found: ${contractId} at ${blockHeight} block height`);
        }
    };

    const contractBlockHeight = await storageClient.getLatestDataBlockHeight(contractCodeKey, blockHeight);
    debug('contract blockHeight', contractBlockHeight);
    if (!contractBlockHeight) {
        await checkAccountExists();
        throw new FastNEARError('codeNotFound', `Cannot find contract code: ${contractId} ${blockHeight}`);
    }

    // TODO: Have cache based on code hash instead?
    const cacheKey = `${contractId}:${contractBlockHeight.toString('hex')}}`;
    let wasmModule = contractCache.get(cacheKey);
    if (wasmModule) {
        debug('contract cache hit', cacheKey);
    } else {
        debug('contract cache miss', cacheKey);

        const wasmData = await storageClient.getData(contractCodeKey, contractBlockHeight);
        if (!wasmData) {
            await checkAccountExists();
            throw new FastNEARError('codeNotFound', `Cannot find contract code: ${contractId} ${blockHeight}`);
        }
        debug('wasmData.length', wasmData.length);
        
        const { prepareWASM } = require('./utils/prepare-wasm');
        const newData = prepareWASM(wasmData);

        debug('wasm compile');
        wasmModule = await WebAssembly.compile(newData);
        contractCache.set(cacheKey, wasmModule);
        debug('wasm compile done');
    }

    return wasmModule;
}

async function runContract(contractId, methodName, methodArgs, blockHeight) {
    const debug = require('debug')(`host:${contractId}`);
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

    const wasmModule = await getWasmModule(contractId, blockHeight);

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
    getWasmModule,
    runContract,
    closeWorkerPool,
};