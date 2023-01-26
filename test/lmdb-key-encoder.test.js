const test = require('tape');

const { KeyEncoder } = require('../storage/lmdb-embedded');

const { writeKey, readKey } = new KeyEncoder();

// NOTE: Using async for sync tests removes the need to use t.plan or t.end

function roundtrip(t, key) {
    const buffer = Buffer.alloc(1024);
    const offset = writeKey(key, buffer, 0);
    const result = readKey(buffer, 0, offset);
    t.deepEquals(result, key);
}

const BUFFER_INCLUDING_ZEROS = Buffer.from([0x1, 0x0, 0xFF, 0x0, 0x2]);

test('string key', async t => roundtrip(t, 'Hello, World!'));
test('empty string key', async t => roundtrip(t, ''));
test('buffer key', async t => roundtrip(t, BUFFER_INCLUDING_ZEROS));
test('empty buffer key', async t => roundtrip(t, Buffer.alloc(0)));
test('change key', async t => roundtrip(t, { compKey: BUFFER_INCLUDING_ZEROS, blockHeight: 123 }));
test('change key: blockHeight = 0', async t => roundtrip(t, { compKey: BUFFER_INCLUDING_ZEROS, blockHeight: 0 }));
test('change key: compKey = empty', async t => roundtrip(t, { compKey: Buffer.alloc(0), blockHeight: 123 }));