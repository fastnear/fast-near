const { EventEmitter } = require('events');
const { Worker } = require('worker_threads');

const kTaskInfo = Symbol('kTaskInfo');
const kWorkerFreedEvent = Symbol('kWorkerFreedEvent');

// NOTE: Mostly lifted from here https://amagiacademy.com/blog/posts/2021-04-09/node-worker-threads-pool
class WorkerPool extends EventEmitter {
    constructor(numThreads, storageClient) {
        super();
        this.numThreads = numThreads;
        this.workers = [];
        this.freeWorkers = [];
        this.storageClient = storageClient;

        for (let i = 0; i < numThreads; i++) {
            this.addNewWorker();
        }
    }

    addNewWorker() {
        const worker = new Worker('./worker.js');
        worker.on('message', ({ result, error, methodName, redisKey }) => {
            const { resolve, reject, blockHeight } = worker[kTaskInfo];

            if (!methodName) {
                worker[kTaskInfo] = null;
                this.freeWorkers.push(worker);
                this.emit(kWorkerFreedEvent);
            }

            if (error) {
                return reject(error);
            }

            if (result) {
                return resolve(result);
            }

            switch (methodName) {
                case 'storage_read':
                    // TODO: Should be possible to coalesce parallel reads to the same key? Or will caching on HTTP level be enough?
                    (async () => {
                        const blockHash = await this.storageClient.getLatestDataBlockHash(redisKey, blockHeight);

                        if (blockHash) {
                            const data = await this.storageClient.getData(redisKey, blockHash);
                            worker.postMessage(data);
                        } else {
                            worker.postMessage(null);
                        }
                    })();
                    break;
            }
        });
        worker.once('exit', (code) => {
            if (code !== 0) {
                console.error(`Worker stopped with exit code ${code}`);
                process.exit(code);
            }
        });
        worker.on('error', (err) => {
            if (worker[kTaskInfo]) {
                worker[kTaskInfo].reject(err)
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

    runContract(blockHeight, wasmModule, contractId, methodName, methodArgs) {
        return new Promise((resolve, reject) => { 
            if (this.freeWorkers.length === 0) {
                // No free threads, wait until a worker thread becomes free.
                // TODO: Throw (for rate limiting) if there are too many queued callbacks
                this.once(kWorkerFreedEvent,
                    () => this.runContract(blockHeight, wasmModule, contractId, methodName, methodArgs).then(resolve).catch(reject));
                return;
            }

            const worker = this.freeWorkers.pop();
            worker[kTaskInfo] = { resolve, reject, blockHeight };
            worker.postMessage({ wasmModule, contractId, methodName, methodArgs });
        });
    }

    close() {
        for (const worker of this.workers) {
            worker.terminate();
        }
    }
}

module.exports = WorkerPool;