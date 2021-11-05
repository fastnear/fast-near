const notImplemented = (...args) => {
    console.error('method not implemented');
    console.debug('args', args);
    console.trace();
    throw new Error('method not implemented');
};

const imports = {
    env: {
        input: notImplemented,
        register_len: notImplemented,
        read_register: notImplemented,
        value_return: notImplemented,
        panic: notImplemented,
        abort: notImplemented,
    }
}

const fs = require('fs');
const wasmData = new Uint8Array(fs.readFileSync('./web4.wasm'));

(async function() {
    console.time('wasm compile');
    const wasmModule = await WebAssembly.compile(wasmData);
    console.timeEnd('wasm compile');

    console.time('module instantiate');
    const wasm2 = await WebAssembly.instantiate(wasmModule, imports);
    console.timeEnd('module instantiate');

    console.time('wasm instantiate');
    const wasm = await WebAssembly.instantiate(wasmData, imports);
    console.timeEnd('wasm instantiate');
})();