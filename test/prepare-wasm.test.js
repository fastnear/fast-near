const test = require('tape');

const fs = require('fs');
const { prepareWASM } = require('../utils/prepare-wasm');

// NOTE: Using async for sync tests removes the need to use t.plan or t.end

function loadModule(wasmData) {
    const wasmModule = new WebAssembly.Module(wasmData);
    const imports = WebAssembly.Module.imports(wasmModule);
    const exports = WebAssembly.Module.exports(wasmModule);
    return { wasmModule, imports, exports };
}

test('remove memory export', async t => {
    const wasmData = fs.readFileSync('./test/data/memory-export.wasm');
    const newData = prepareWASM(wasmData);

    const { imports, exports } = loadModule(newData);
    t.true(imports.some(i => i.module === 'env' && i.name === 'memory'));
    t.false(exports.some(e => e.kind === 'memory'));
});

test('replace existing memory import', async t => {
    const wasmData = fs.readFileSync('./test/data/imported_memory.wasm');
    const newData = prepareWASM(wasmData);

    const { imports } = loadModule(newData);
    t.true(imports.some(i => i.module === 'env' && i.name === 'memory'));
    // TODO: What else to assert? Maybe check output binary match?
});