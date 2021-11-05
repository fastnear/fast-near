// NOTE: Needs --experimental-wasm-bigint 

const notImplemented = (name) => (...args) => {
    console.debug('notImplemented', name, 'args', args);
    throw new Error('method not implemented: ' + name);
};

const registers = {};

const inputArgs = JSON.stringify({
    // TODO
});

const MAX_U64 = 18446744073709551615n;

const imports = (ctx) => ({
    env: {
        input: (register_id) => {
            registers[register_id] = Buffer.from(inputArgs);
        },
        register_len: (register_id) => {
            return BigInt(registers[register_id] ? registers[register_id].length : MAX_U64);
        },
        read_register: (register_id, ptr) => {
            notImplemented('read');
        },
        value_return: notImplemented('value_return'),
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

const fs = require('fs');
const wasmData = new Uint8Array(fs.readFileSync('./web4.wasm'));

(async function() {
    console.time('wasm compile');
    const wasmModule = await WebAssembly.compile(wasmData);
    console.timeEnd('wasm compile');

    console.time('module instantiate');
    const ctx2 = {};
    const wasm2 = await WebAssembly.instantiate(wasmModule, imports(ctx2));
    ctx2.memory = wasm2.exports.memory;
    console.log('exports', wasm2.exports.memory);
    wasm2.exports.memory
    wasm2.exports.web4_get();
    console.timeEnd('module instantiate');

    console.time('wasm instantiate');
    const ctx = {};
    const wasm = await WebAssembly.instantiate(wasmData, imports(ctx));
    ctx.memory = wasm2.exports.memory;
    wasm.exports.web4_get();
    console.timeEnd('wasm instantiate');
})();