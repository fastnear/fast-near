const {
    parentPort, workerData, receiveMessageOnPort, threadId
} = require('worker_threads');

const debug = require('debug')(`worker:${threadId}`);

const prettyBuffer = require('./utils/pretty-buffer');
const imports = require('./runtime/view-only');

async function runWASM({ blockHeight, blockTimestamp, accountBalance, accountLockedBalance, storageUsage, wasmModule, contractId, methodName, methodArgs }) {
    debug('runWASM', contractId, methodName, prettyBuffer(Buffer.from(methodArgs)));
    // TODO: Take memory size from config
    const memory = new WebAssembly.Memory({ initial: 1024, maximum: 2048 });
    const ctx = {
        registers: {},
        blockHeight,
        blockTimestamp,
        accountBalance,
        accountLockedBalance,
        storageUsage,
        contractId,
        methodArgs,
        logs: [],
        result: Buffer.from([]),
        threadId,
        parentPort,
        receiveMessageOnPort,
    };
    debug('module instantiate');
    const wasmInstance = await WebAssembly.instantiate(wasmModule, { env: { ...imports(ctx), memory } });
    debug('module instantiate done');
    ctx.memory = memory;
    try {
        debug(`run ${methodName}`);
        wasmInstance.exports[methodName]();
    } finally {
        debug(`run ${methodName} done`);
    }

    return ctx;
}

parentPort.on('message', message => {
    if (message.wasmModule) {
        runWASM(message).then(({ result, logs }) => {
            parentPort.postMessage({ result, logs });
        }).catch(error => {
            parentPort.postMessage({ error, errorCode: error.code });
        });
    }
});
