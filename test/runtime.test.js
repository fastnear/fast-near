const test = require('tape');
const { readFileSync } = require('fs');

const imports = require('../runtime/view-only');
const { FastNEARError } = require('../error');

// Helper function to create a context
function createContext() {
    return {
        registers: {},
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
    ctx.registers[0] = Buffer.from(ctx.methodArgs);
    t.equal(Number(importFunctions.register_len(0)), ctx.registers[0].length);
    t.equal(Number(importFunctions.register_len(1)), 18446744073709551615);
});

test('read_register copies data to memory', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    ctx.registers[0] = Buffer.from(ctx.methodArgs);
    const ptr = 0;
    importFunctions.read_register(0, ptr);
    const result = new Uint8Array(ctx.memory.buffer, ptr, ctx.registers[0].length);
    t.equal(Buffer.from(result).toString(), ctx.registers[0].toString());
});

test('current_account_id sets correct value', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    importFunctions.current_account_id(0);
    t.equal(ctx.registers[0].toString(), ctx.contractId);
});

test('input sets correct value', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    importFunctions.input(0);
    t.equal(ctx.registers[0].toString(), ctx.methodArgs);
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
    t.equal(ctx.registers[0].toString('hex'), expectedHash.toString('hex'));
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
    t.equal(ctx.registers[0].toString(), 'testValue');
});

test('prohibited methods throw FastNEARError', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    const prohibitedMethods = [
        'signer_account_id', 'signer_account_pk', 'predecessor_account_id',
        'attached_deposit', 'prepaid_gas', 'used_gas',
        'promise_create', 'promise_then', 'promise_and', 'promise_batch_create',
        'promise_batch_then', 'promise_batch_action_create_account',
        'promise_batch_action_deploy_contract', 'promise_batch_action_function_call',
        'promise_batch_action_function_call_weight', 'promise_batch_action_transfer',
        'promise_batch_action_stake', 'promise_batch_action_add_key_with_full_access',
        'promise_batch_action_add_key_with_function_call', 'promise_batch_action_delete_key',
        'promise_batch_action_delete_account', 'promise_results_count',
        'promise_result', 'promise_return', 'storage_write', 'storage_remove'
    ];
    prohibitedMethods.forEach(method => {
        t.throws(() => importFunctions[method](), FastNEARError, `${method} should throw FastNEARError`);
    });
});

test('not implemented methods throw FastNEARError', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx)
    const notImplementedMethods = [
        'epoch_height', 'storage_usage', 'account_balance', 'account_locked_balance',
        'random_seed', 
        'validator_stake', 'validator_total_stake', 'alt_bn128_g1_multiexp',
        'alt_bn128_g1_sum', 'alt_bn128_pairing_check'
    ];
    notImplementedMethods.forEach(method => {
        t.throws(() => importFunctions[method](), FastNEARError, `${method} should throw FastNEARError`);
    });
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

test('keccak256 calculates correct hash', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    const testData = 'test data';
    const dataArray = Buffer.from(testData);
    const inputPtr = 0;
    new Uint8Array(ctx.memory.buffer).set(dataArray, inputPtr);

    importFunctions.keccak256(testData.length, inputPtr, 0);

    const expectedHash = '7d92c840d5f0ac4f83543201db6005d78414059c778169efa3760f67a451e7ef';
    t.equal(ctx.registers[0].toString('hex'), expectedHash);
});

test('keccak512 calculates correct hash', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    const testData = 'test data';
    const dataArray = Buffer.from(testData);
    const inputPtr = 0;
    new Uint8Array(ctx.memory.buffer).set(dataArray, inputPtr);

    importFunctions.keccak512(testData.length, inputPtr, 0);

    const expectedHash = '8ec47653f62877c90050f315b0526b778d90e81cef33d12c18fea17a97bf614f9d06789819a7583a4d3e9d831d331a6340b443158156c0bf52b8d85a6b2462dc';
    t.equal(ctx.registers[0].toString('hex'), expectedHash);
});

test('ripemd160 calculates correct hash', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    const testData = 'test data';
    const dataArray = Buffer.from(testData);
    const inputPtr = 0;
    new Uint8Array(ctx.memory.buffer).set(dataArray, inputPtr);

    importFunctions.ripemd160(testData.length, inputPtr, 0);

    const expectedHash = 'feaf1fb8e0a8cd67d52ac4b437cd0660addd947b';
    t.equal(ctx.registers[0].toString('hex'), expectedHash);
});

test('ecrecover returns correct public key and result', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    
    const testsPath = './test/data/ecrecover-tests.json';
    const tests = JSON.parse(readFileSync(testsPath, 'utf8'));

    for (let i = 1; i <= tests.length; i++) {
        // if (i !== 120) continue;

        const { m, v, sig, mc, res } = tests[i - 1];
        const hash = Buffer.from(m, 'hex');
        const signature = Buffer.from(sig, 'hex');
        
        const hashPtr = 0;
        const signaturePtr = hashPtr + hash.length;
        const registerID = 1;

        new Uint8Array(ctx.memory.buffer).set(hash, hashPtr);
        new Uint8Array(ctx.memory.buffer).set(signature, signaturePtr);

        const result = importFunctions.ecrecover(hash.length, hashPtr, signature.length, signaturePtr, v, mc ? 1 : 0, registerID);
        
        if (res) {
            t.equal(result, 1n, `Test ${i}: ecrecover should return 1 for valid input`);
            t.equal(ctx.registers[registerID]?.length, 64, `Test ${i}: Recovered public key should be 64 bytes`);
            t.deepEqual(ctx.registers[registerID], Buffer.from(res, 'hex'), `Test ${i}: Recovered public key should match expected result`);
        } else {
            t.equal(result, 0n, `Test ${i}: ecrecover should return 0 for invalid input`);
        }
    }
});

test('ed25519_verify returns correct value', async t => {
    const ctx = createContext();
    const importFunctions = imports(ctx);
    const message = Buffer.from('test message');
    const publicKey = Buffer.from('38e69cc61ca9f9d1554c0be0c14856e1ba26ea47ce8b1e4f76a0a4822301cc9b', 'hex');
    const signature = Buffer.from('e14e0c6fd9a703da54fa09ef882559d4376e8827f1224aa675af344290cfa7fdee24a3960d7cfaf23de3be8ee12b1d909331e50375d36763d9a531b7d5091d07', 'hex');

    const messagePtr = 0;
    const publicKeyPtr = 64;
    const signaturePtr = 128;

    new Uint8Array(ctx.memory.buffer).set(message, messagePtr);
    new Uint8Array(ctx.memory.buffer).set(publicKey, publicKeyPtr);
    new Uint8Array(ctx.memory.buffer).set(signature, signaturePtr);

    const result = importFunctions.ed25519_verify(signature.length, signaturePtr, message.length, messagePtr, publicKey.length, publicKeyPtr);
    t.equal(result, 1n, 'ed25519_verify should return 1 for valid signature');

    // Test with invalid message
    const invalidMessage = Buffer.from(message);
    invalidMessage[0] ^= 1; // Flip one bit to make it invalid
    new Uint8Array(ctx.memory.buffer).set(invalidMessage, messagePtr);
    const invalidMessageResult = importFunctions.ed25519_verify(signature.length, signaturePtr, invalidMessage.length, messagePtr, publicKey.length, publicKeyPtr);
    t.equal(invalidMessageResult, 0n, 'ed25519_verify should return 0 for invalid message');

    // Test with invalid signature
    const invalidSignature = Buffer.from(signature);
    invalidSignature[0] ^= 1; // Flip one bit to make it invalid
    new Uint8Array(ctx.memory.buffer).set(invalidSignature, signaturePtr);
    const invalidSignatureResult = importFunctions.ed25519_verify(invalidSignature.length, signaturePtr, message.length, messagePtr, publicKey.length, publicKeyPtr);
    t.equal(invalidSignatureResult, 0n, 'ed25519_verify should return 0 for invalid signature');
});