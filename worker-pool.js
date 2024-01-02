const { EventEmitter } = require('events');
const { Worker } = require('worker_threads');

const { FastNEARError } = require('./error');

const kTaskInfo = Symbol('kTaskInfo');
const kWorkerFreedEvent = Symbol('kWorkerFreedEvent');

const CONTRACT_TIMEOUT_MS = parseInt(process.env.FAST_NEAR_CONTRACT_TIMEOUT_MS || '1000');

// NOTE: Mostly lifted from here https://amagiacademy.com/blog/posts/2021-04-09/node-worker-threads-pool
class WorkerPool extends EventEmitter {
    constructor(numThreads, storage) {
        super();
        this.numThreads = numThreads;
        this.workers = [];
        this.freeWorkers = [];
        this.storage = storage;
        this.running = true;

        for (let i = 0; i < numThreads; i++) {
            this.addNewWorker();
        }
    }

    addNewWorker() {
        const worker = new Worker(`${__dirname}/worker.js`);
        worker.on('message', ({ result, logs, error, errorCode, methodName, compKey }) => {
            let { resolve, reject, blockHeight } = worker[kTaskInfo];

            if (!methodName) {
                clearTimeout(worker[kTaskInfo].timeoutHandle);
                worker[kTaskInfo] = null;
                this.freeWorkers.push(worker);
                this.emit(kWorkerFreedEvent);
            }

            if (error) {
                if (errorCode) {
                    // TODO: Should we preserve call stack when possible?
                    return reject(new FastNEARError(errorCode, error.message));
                }
                return reject(error);
            }

            if (!methodName) {
                return resolve({ result, logs });
            }

            compKey = Buffer.from(compKey);
            blockHeight = Buffer.from(blockHeight.toString());

            switch (methodName) {
                case 'storage_read':
                    (async () => {
                        const data = await this.storage.getLatestData(compKey, blockHeight);
                        worker.postMessage(data);
                    })().catch((error) => {
                        worker.postMessage({ error });
                    });
                    break;
            }
        });
        worker.once('exit', (code) => {
            worker.emit('error', new Error(`Worker stopped with exit code ${code}`));
        });
        worker.on('error', (err) => {
            if (!this.running) {
                return;
            }

            if (worker[kTaskInfo]) {
                const { contractId, methodName, didTimeout, reject } = worker[kTaskInfo]
                if (didTimeout) {
                    err = new FastNEARError('executionTimedOut', `${contractId}.${methodName} execution timed out`, { accountId: contractId, methodName });
                }
                reject(err)
            } else {
                this.emit('error', err);
            }
            this.workers.splice(this.workers.indexOf(worker), 1);
            this.addNewWorker();
        });
        this.workers.push(worker);
        this.freeWorkers.push(worker);
        this.emit(kWorkerFreedEvent);
    }

    runContract(blockHeight, blockTimestamp, wasmModule, contractId, methodName, methodArgs) {
        return new Promise((resolve, reject) => { 
            if (this.freeWorkers.length === 0) {
                // No free threads, wait until a worker thread becomes free.
                // TODO: Throw (for rate limiting) if there are too many queued callbacks
                this.once(kWorkerFreedEvent,
                    () => this.runContract(blockHeight, blockTimestamp, wasmModule, contractId, methodName, methodArgs).then(resolve).catch(reject));
                return;
            }

            const worker = this.freeWorkers.pop();
            worker[kTaskInfo] = { resolve, reject, blockHeight, blockTimestamp, contractId, methodName };
            worker.postMessage({ wasmModule, blockHeight, blockTimestamp, contractId, methodName, methodArgs });
            worker[kTaskInfo].timeoutHandle = setTimeout(() => {
                if (worker[kTaskInfo]) {
                    worker[kTaskInfo].didTimeout = true;
                    worker.terminate();
                }
            }, CONTRACT_TIMEOUT_MS);
        });
    }

    close() {
        this.running = false;
        for (const worker of this.workers) {
            worker.terminate();
        }
        this.workers = [];
    }
}

module.exports = WorkerPool;