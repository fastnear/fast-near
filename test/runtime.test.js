const test = require('tape');
const imports = require('../runtime/view-only');
const { FastNEARError } = require('../error');

// Helper function to create a context
function createContext() {
    return {
        threadId: 'test-thread',
        memory: {
            buffer: new ArrayBuffer(8 * 1024),
        },
        contractId: 'test.near',
        blockHeight: 12345,
        blockTimestamp: 1234567890,
        methodArgs: '{"some":"args"}',
        logs: [],
        parentPort: {
            postMessage: () => { },
        },
        receiveMessageOnPort: () => { },
    };
}

test('register_len returns correct length', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    importFunctions.input(0);
    t.equal(Number(importFunctions.register_len(0)), ctx.methodArgs.length);
    t.equal(Number(importFunctions.register_len(1)), 18446744073709551615);
});

test('read_register copies data to memory', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    importFunctions.input(0);
    const ptr = 0;
    importFunctions.read_register(0, ptr);
    const result = new Uint8Array(ctx.memory.buffer, ptr, ctx.methodArgs.length);
    t.equal(Buffer.from(result).toString(), ctx.methodArgs);
});

test('current_account_id sets correct value', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    importFunctions.current_account_id(0);
    t.equal(Number(importFunctions.register_len(0)), ctx.contractId.length);
});

test('input sets correct value', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    importFunctions.input(0);
    t.equal(Number(importFunctions.register_len(0)), ctx.methodArgs.length);
});

test('block_index returns correct value', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    t.equal(Number(importFunctions.block_index()), ctx.blockHeight);
});

test('block_timestamp returns correct value', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    t.equal(Number(importFunctions.block_timestamp()), ctx.blockTimestamp);
});

test('sha256 calculates correct hash', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    const testData = 'test data';
    const dataArray = Buffer.from(testData);
    const ptr = 0;
    new Uint8Array(ctx.memory.buffer).set(dataArray, ptr);

    importFunctions.sha256(testData.length, ptr, 0);

    const expectedHash = Buffer.from('916f0027a575074ce72a331777c3478d6513f786a591bd892da1a577bf2335f9', 'hex');
    t.equal(Number(importFunctions.register_len(0)), expectedHash.length);
});

test('value_return sets result correctly', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    const testData = 'return value';
    const dataArray = Buffer.from(testData);
    const ptr = 0;
    new Uint8Array(ctx.memory.buffer).set(dataArray, ptr);

    importFunctions.value_return(testData.length, ptr);

    t.equal(ctx.result.toString(), testData);
});

test('log_utf8 adds log message', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    const testMessage = 'Test log message';
    const dataArray = Buffer.from(testMessage);
    const ptr = 0;
    new Uint8Array(ctx.memory.buffer).set(dataArray, ptr);

    importFunctions.log_utf8(testMessage.length, ptr);

    t.ok(ctx.logs.includes(testMessage));
});

test('storage_read calls parentPort and returns correct value', async t => {
    const ctx = createContext();
    ctx.receiveMessageOnPort = () => ({ message: Buffer.from('testValue') });
    const importFunctions = imports(ctx);
    const testKey = 'testKey';
    const dataArray = Buffer.from(testKey);
    const ptr = 0;
    new Uint8Array(ctx.memory.buffer).set(dataArray, ptr);

    const result = importFunctions.storage_read(testKey.length, ptr, 0);

    t.equal(Number(result), 1);
    t.equal(Number(importFunctions.register_len(0)), 9); // 'testValue'.length
});

test('prohibited methods throw FastNEARError', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    t.throws(() => importFunctions.signer_account_id(), FastNEARError);
    t.throws(() => importFunctions.attached_deposit(), FastNEARError);
    t.throws(() => importFunctions.promise_create(), FastNEARError);
});

test('not implemented methods throw FastNEARError', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    t.throws(() => importFunctions.epoch_height(), FastNEARError);
    t.throws(() => importFunctions.storage_usage(), FastNEARError);
    t.throws(() => importFunctions.validator_stake(), FastNEARError);
});

test('panic throws FastNEARError', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    t.throws(() => importFunctions.panic(), FastNEARError);
});

test('panic_utf8 throws FastNEARError with correct message', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    const testMessage = 'Test panic message';
    const dataArray = Buffer.from(testMessage);
    const ptr = 0;
    new Uint8Array(ctx.memory.buffer).set(dataArray, ptr);

    try {
        importFunctions.panic_utf8(testMessage.length, ptr);
    } catch (error) {
        t.ok(error instanceof FastNEARError);
        t.equal(error.message, testMessage);
    }
});

test('abort throws FastNEARError with correct message', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    const testMessage = 'Test abort message';
    const testFilename = 'test.js';
    const msgPtr = 4;
    const filenamePtr = 100;
    const line = 42;
    const col = 10;

    // Set message length (4 bytes before the actual message)
    new Uint32Array(ctx.memory.buffer, 0, 1)[0] = testMessage.length;
    // Set filename length (4 bytes before the actual filename)
    new Uint32Array(ctx.memory.buffer, 96, 1)[0] = testFilename.length;

    // Set message and filename in UTF-16
    const msgBuffer = Buffer.from(testMessage, 'utf16le');
    const filenameBuffer = Buffer.from(testFilename, 'utf16le');
    new Uint8Array(ctx.memory.buffer).set(msgBuffer, msgPtr);
    new Uint8Array(ctx.memory.buffer).set(filenameBuffer, filenamePtr);

    try {
        importFunctions.abort(msgPtr, filenamePtr, line, col);
        t.fail('abort should throw an error');
    } catch (error) {
        t.ok(error instanceof FastNEARError);
        t.equal(error.message, `${testMessage}, filename: "${testFilename}" line: ${line} col: ${col}`);
    }

    t.ok(ctx.logs.includes(`ABORT: ${testMessage}, filename: "${testFilename}" line: ${line} col: ${col}`));
});

test('log_utf16 adds log message', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    const testMessage = 'Test log message';
    const dataArray = Buffer.from(testMessage, 'utf16le');
    const ptr = 0;
    new Uint8Array(ctx.memory.buffer).set(dataArray, ptr);

    importFunctions.log_utf16(testMessage.length, ptr);

    t.ok(ctx.logs.includes(testMessage));
});

test('storage_has_key returns correct value', async t => {
    const ctx = createContext();
    ctx.receiveMessageOnPort = () => ({ message: Buffer.from('testValue') });
    const importFunctions = imports(ctx);
    const testKey = 'testKey';
    const dataArray = Buffer.from(testKey);
    const ptr = 0;
    new Uint8Array(ctx.memory.buffer).set(dataArray, ptr);

    const result = importFunctions.storage_has_key(testKey.length, ptr);

    t.equal(Number(result), 1);

    ctx.receiveMessageOnPort = () => ({ message: null });
    const resultNotFound = importFunctions.storage_has_key(testKey.length, ptr);

    t.equal(Number(resultNotFound), 0);
});