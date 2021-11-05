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

const fs = require('fs'),
    wasm = WebAssembly.instantiate(new Uint8Array(fs.readFileSync('./web4.wasm')), imports)
        .then(result => console.log(result));
