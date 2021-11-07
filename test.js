// NOTE: Needs --experimental-wasm-bigint

const notImplemented = (name) => (...args) => {
    console.debug('notImplemented', name, 'args', args);
    throw new Error('method not implemented: ' + name);
};

const registers = {};

const MAX_U64 = 18446744073709551615n;

const {
    Worker, isMainThread, parentPort, workerData, receiveMessageOnPort
} = require('worker_threads');

const imports = (ctx) => {
    function readUTF16CStr(ptr) {
        let arr = [];
        const mem = new Uint16Array(ctx.memory.buffer);
        ptr = Number(ptr) / 2;
        while (mem[ptr] != 0) {
            arr.push(mem[ptr]);
            ptr++;
        }
        return Buffer.from(Uint16Array.from(arr).buffer).toString('ucs2');
    }

    return {
        env: {
            input: (register_id) => {
                registers[register_id] = Buffer.from(ctx.args);
            },
            register_len: (register_id) => {
                return BigInt(registers[register_id] ? registers[register_id].length : MAX_U64);
            },
            read_register: (register_id, ptr) => {
                const mem = new Uint8Array(ctx.memory.buffer)
                mem.set(registers[register_id] || Buffer.from([]), Number(ptr));
            },
            value_return: (value_len, value_ptr) => {
                const mem = new Uint8Array(ctx.memory.buffer)
                ctx.result = Buffer.from(mem.slice(Number(value_ptr), Number(value_ptr + value_len)));
            },
            current_account_id: (register_id) => {
                // TODO: What is proper account ID for view calls?
                registers[register_id] = Buffer.from('');
            },
            predecessor_account_id: (register_id) => {
                // TODO: What is proper account ID for view calls?
                registers[register_id] = Buffer.from('');
            },
            storage_read: (key_len, key_ptr, register_id) => {
                const storageKey = Buffer.from(new Uint8Array(ctx.memory.buffer, Number(key_ptr), Number(key_len)));
                const redisKey = Buffer.concat([Buffer.from(`data:${ctx.contractId}:`), storageKey]);
                console.log('storage_read', ctx.contractId, storageKey.toString('utf8'));

                parentPort.postMessage({
                    methodName: 'storage_read',
                    redisKey
                });

                let resultMessage
                do {
                    resultMessage = receiveMessageOnPort(parentPort);
                } while (!resultMessage);
                const result = resultMessage.message;

                if (!result) {
                    return 0;
                }

                registers[register_id] = result;
                console.log('storage_read result', Buffer.from(result).toString('utf8'));
                return 1n;
            },
            storage_write: notImplemented('storage_write'),
            attached_deposit: notImplemented('attached_deposit'),
            promise_batch_create: notImplemented('promise_batch_create'),
            promise_batch_action_transfer: notImplemented('promise_batch_action_transfer'),
            panic: notImplemented('panic'),
            abort: (msg_ptr, filename_ptr, line, col) => {
                console.error('abort', readUTF16CStr(msg_ptr), readUTF16CStr(filename_ptr), line, col);
                throw new Error('abort');
            }
        }
    }
};

const { createClient } = require('redis');

async function runContract(contractId, methodName, args) {
    const client = createClient();
    client.on('error', (err) => console.log('Redis Client Error', err));
    await client.connect();

    const latestBlockHeight = await client.get('latest_block_height');
    console.log('latestBlockHeight', latestBlockHeight)

    const [contractBlockHash] = await client.sendCommand(['ZREVRANGEBYSCORE',
        `code:${contractId}`, latestBlockHeight, '-inf', 'LIMIT', '0', '1'], {}, true);

    const wasmData = await client.getBuffer(Buffer.concat([Buffer.from(`code:${contractId}:`), contractBlockHash]));
    console.log('wasmData', wasmData.length);

    console.time('wasm compile');
    const wasmModule = await WebAssembly.compile(wasmData);
    console.timeEnd('wasm compile');

    const result = await new Promise((resolve, reject) => {
        const worker = new Worker(__filename, {
            workerData: {
                wasmModule,
                contractId,
                methodName,
                args
            }
        });
        worker.on('message', message => {
            if (!message.methodName) {
                resolve(message.result);
            }
            
            switch (message.methodName) {
                case 'storage_read':
                    const { redisKey } = message;
                    (async () => {
                        const [blockHash] = await client.sendCommand(['ZREVRANGEBYSCORE',
                            redisKey, latestBlockHeight, '-inf', 'LIMIT', '0', '1'], {}, true);

                        const data = await client.getBuffer(Buffer.concat([redisKey, Buffer.from(':'), blockHash]));
                        worker.postMessage(data);
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

async function runWASM({ wasmModule, contractId, methodName, args }) {
    const ctx = {
        contractId,
        args: JSON.stringify(args)
    };
    console.time('module instantiate');
    const wasm2 = await WebAssembly.instantiate(wasmModule, imports(ctx));
    console.timeEnd('module instantiate');
    ctx.memory = wasm2.exports.memory;
    console.time(`run ${methodName}`);
    wasm2.exports[methodName]();
    console.timeEnd(`run ${methodName}`);

    return ctx.result;
}

(async function() {
    if (isMainThread) {
        const result = await runContract('dev-1629863402519-20649210409803', 'getChunk', {x: 0, y: 0});
        console.log('runContract result', Buffer.from(result).toString('utf8'));
    } else {
        console.log('workerData', workerData);
        parentPort.postMessage({
            result: await runWASM(workerData)
        });
    }
})();