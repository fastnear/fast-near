const WorkerPool = require('./worker-pool');
const storage = require('./storage');
const { FastNEARError } = require('./error');

const WORKER_COUNT = parseInt(process.env.FAST_NEAR_WORKER_COUNT || '4');
const NO_CODE_HASH = Buffer.from('11111111111111111111111111111111', 'hex');

const { accountKey } = require('./storage-keys');
const { BORSH_SCHEMA, Account } = require('./data-model');
const { deserialize } = require('borsh');
const bs58 = require('bs58');

const LRU = require('lru-cache');
let contractCache = new LRU({
    max: 25
});

let workerPool;

async function getWasmModule(accountId, blockHeight) {
    const debug = require('debug')(`host:${accountId}`);
    debug('getWasmModule', accountId, blockHeight);

    const accountDataKey = accountKey(accountId);

    debug('load account data')
    const accountData = await storage.getLatestData(accountDataKey, blockHeight);
    debug('accountData', accountData);
    if (!accountData) {
        throw new FastNEARError('accountNotFound', `Account not found: ${accountId} at ${blockHeight} block height`, { accountId, blockHeight });
    }

    const account = deserialize(BORSH_SCHEMA, Account, accountData);
    const { code_hash } = account;

    const codeHash = Buffer.from(code_hash);
    const codeHashStr = bs58.encode(codeHash);

    if (NO_CODE_HASH.equals(codeHash)) {
        throw new FastNEARError('codeNotFound', `Cannot find contract code: ${accountId}, block height: ${blockHeight}, code hash: ${codeHashStr}`, { accountId, blockHeight, codeHashStr });
    }

    const cacheKey = codeHashStr;
    let wasmModule = contractCache.get(cacheKey);
    if (wasmModule) {
        debug('contract cache hit', cacheKey);
    } else {
        debug('contract cache miss', cacheKey);

        const wasmData = await storage.getBlob(codeHash);
        if (!wasmData) {
            // TODO: Should this be fatal error because shoudn't happen with consistent data?
            throw new FastNEARError('codeNotFound', `Cannot find contract code: ${accountId}, block height: ${blockHeight}, code hash: ${codeHashStr}`, { accountId, blockHeight, codeHashStr });
        }
        debug('wasmData.length', wasmData.length);
        
        const { prepareWASM } = require('./utils/prepare-wasm');
        const newData = prepareWASM(wasmData);

        debug('wasm compile');
        wasmModule = await WebAssembly.compile(newData);
        contractCache.set(cacheKey, wasmModule);
        debug('wasm compile done');
    }

    return { wasmModule, account };
}

async function runContract(contractId, methodName, methodArgs, blockHeight) {
    const debug = require('debug')(`host:${contractId}`);
    debug('runContract', contractId, methodName, methodArgs, blockHeight);

    if (!Buffer.isBuffer(methodArgs)) {
        methodArgs = Buffer.from(JSON.stringify(methodArgs));
    }

    if (!workerPool) {
        debug('workerPool');
        workerPool = new WorkerPool(WORKER_COUNT, storage);
        debug('workerPool done');
    }

    const blockTimestamp = await storage.getBlockTimestamp(blockHeight);
    debug('blockTimestamp', blockTimestamp);

    const { wasmModule, account } = await getWasmModule(contractId, blockHeight);

    debug('worker start');
    const { result, logs } = await workerPool.runContract(blockHeight, blockTimestamp, wasmModule, account, contractId, methodName, methodArgs);
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