const {
    parentPort, workerData, receiveMessageOnPort
} = require('worker_threads');

const MAX_U64 = 18446744073709551615n;

const notImplemented = (name) => (...args) => {
    console.debug('notImplemented', name, 'args', args);
    throw new Error('method not implemented: ' + name);
};

const imports = (ctx) => {
    const registers = {};

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
                    return 0n;
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

async function runWASM({ wasmModule, contractId, methodName, args }) {
    const ctx = {
        contractId,
        args: JSON.stringify(args)
    };
    console.time('module instantiate');
    const wasm2 = await WebAssembly.instantiate(wasmModule, imports(ctx));
    console.timeEnd('module instantiate');
    ctx.memory = wasm2.exports.memory;
    try {
        console.time(`run ${methodName}`);
        wasm2.exports[methodName]();
    } finally {
        console.timeEnd(`run ${methodName}`);
    }

    return ctx.result;
}

(async function() {
    console.log('workerData', workerData);
    try {
        parentPort.postMessage({
            result: await runWASM(workerData)
        });
    } catch (error) {
        parentPort.postMessage({ error });
    }
})().catch(error => {
    console.error(error);
    process.exit(1);
});
