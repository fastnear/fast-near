// NOTE: Needs --experimental-wasm-bigint

const notImplemented = (name) => (...args) => {
    console.debug('notImplemented', name, 'args', args);
    throw new Error('method not implemented: ' + name);
};

const registers = {};

const MAX_U64 = 18446744073709551615n;

const {
    Worker, isMainThread, parentPort, workerData
} = require('worker_threads');

const imports = (ctx) => ({
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
            console.log('value_return', Buffer.from(mem.slice(Number(value_ptr), Number(value_ptr + value_len))).toString('utf8'));
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
            // TODO: Read from Redis??
            notImplemented('storage_read')();
        },
        storage_write: notImplemented('storage_write'),
        attached_deposit: notImplemented('attached_deposit'),
        promise_batch_create: notImplemented('promise_batch_create'),
        promise_batch_action_transfer: notImplemented('promise_batch_action_transfer'),
        panic: notImplemented('panic'),
        abort: (msg_ptr, filename_ptr, line, col) => {
            function readUTF16Str(ptr) {
                let arr = [];
                const mem = new Uint16Array(ctx.memory.buffer);
                ptr = ptr / 2;
                while (mem[ptr] != 0) {
                  arr.push(mem[ptr]);
                  ptr++;
                }
                return Buffer.from(Uint16Array.from(arr).buffer).toString('ucs2');
            }

            console.error('abort', readUTF16Str(msg_ptr), readUTF16Str(filename_ptr), line, col);
            throw new Error('abort');
        }
    }
});

const { createClient } = require('redis');

async function runContract(contractId, methodName, args) {
    const client = createClient();
    client.on('error', (err) => console.log('Redis Client Error', err));
    await client.connect();

    const latestBlockHeight = await client.get('latest_block_height');
    console.log('latestBlockHeight', latestBlockHeight, typeof latestBlockHeight)

    const [contractBlockHash] = await client.sendCommand(['ZREVRANGEBYSCORE',
        `code:${contractId}`, latestBlockHeight, '-inf', 'LIMIT', '0', '1'], {}, true);

    const wasmData = await client.getBuffer(Buffer.concat([Buffer.from(`code:${contractId}:`), Buffer.from(contractBlockHash)]));
    console.log('wasmData', wasmData.length);

    console.time('wasm compile');
    const wasmModule = await WebAssembly.compile(wasmData);
    console.timeEnd('wasm compile');

    await runWASMAsync(wasmModule, methodName, args);
}

function runWASMAsync(wasmModule, methodName, args) {   
    return new Promise((resolve, reject) => {
        const worker = new Worker(__filename, {
            workerData: {
                wasmModule,
                methodName,
                args
            }
        });
        worker.on('message', resolve);
        worker.on('error', reject);
        worker.on('exit', (code) => {
            if (code !== 0) {
                reject(new Error(`Worker stopped with exit code ${code}`));
            }
        });

        // TODO: Return value
    });
}

async function runWASM({ wasmModule, methodName, args }) {
    const ctx = {
        args: JSON.stringify(args)
    };
    console.time('module instantiate');
    const wasm2 = await WebAssembly.instantiate(wasmModule, imports(ctx));
    ctx.memory = wasm2.exports.memory;
    console.log('exports', wasm2.exports.memory);
    console.time(`run ${methodName}`);
    wasm2.exports[methodName]();
    console.timeEnd(`run ${methodName}`);
    console.timeEnd('module instantiate');
}

(async function() {
    console.log('isMainThread', isMainThread)
    if (isMainThread) {
        await runContract('dev-1629863402519-20649210409803', 'getChunk', {x: 0, y: 0});
    } else {
        console.log('workerData', workerData);
        parentPort.postMessage(await runWASM(workerData));
    }
})();