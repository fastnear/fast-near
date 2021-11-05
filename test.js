// NOTE: Needs --experimental-wasm-bigint 

const notImplemented = (name) => (...args) => {
    console.debug('notImplemented', name, 'args', args);
    throw new Error('method not implemented');
};

const imports = {
    env: {
        input: notImplemented('input'),
        register_len: notImplemented('register_len'),
        read_register: notImplemented('read_register'),
        value_return: notImplemented('value_return'),
        panic: notImplemented('panic'),
        abort: notImplemented('abort'),
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
    wasm2.exports.web4_get();
    console.timeEnd('module instantiate');

    console.time('wasm instantiate');
    const wasm = await WebAssembly.instantiate(wasmData, imports);
    wasm.exports.web4_get();
    console.timeEnd('wasm instantiate');
})();