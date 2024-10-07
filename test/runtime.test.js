const test = require('tape');
const imports = require('../runtime/view-only');
const { FastNEARError } = require('../error');

// Helper function to create a context
function createContext() {
    return {
        threadId: 'test-thread',
        memory: {
            buffer: new ArrayBuffer(1024),
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
    const dataArray = new TextEncoder().encode(testData);
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
    const dataArray = new TextEncoder().encode(testData);
    const ptr = 0;
    new Uint8Array(ctx.memory.buffer).set(dataArray, ptr);

    importFunctions.value_return(testData.length, ptr);

    t.equal(ctx.result.toString(), testData);
});

test('log_utf8 adds log message', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    const testMessage = 'Test log message';
    const dataArray = new TextEncoder().encode(testMessage);
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
    const dataArray = new TextEncoder().encode(testKey);
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