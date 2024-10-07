const { createHash } = require('crypto');

const { dataKey } = require('../storage-keys');
const prettyBuffer = require('../utils/pretty-buffer');
const { FastNEARError } = require('../error');

const MAX_U64 = 18446744073709551615n;

const imports = (ctx) => {
    const debug = require('debug')(`worker:runtime:${ctx.threadId}`);

    const notImplemented = (name) => (...args) => {
        debug('notImplemented', name, 'args', args);
        throw new FastNEARError('notImplemented', 'method not implemented: ' + name, { methodName: name });
    };

    const prohibitedInView = (name) => (...args) => {
        debug('prohibitedInView', name, 'args', args);
        // TODO: Shouldn't this use unique code which is not resulting in proxyJson?
        throw new FastNEARError('notImplemented', 'method not available for view calls: ' + name, { methodName: name });
    };

    const registers = {};

    // TODO: Need to handle strings with unknown length?
    function readUTF16CStr(len, ptr) {
        let arr = [];
        const mem = new Uint16Array(ctx.memory.buffer, Number(ptr));
        for (let i = 0; i < len && mem[i] != 0; i++) {
            arr.push(mem[i]);
        }
        return Buffer.from(Uint16Array.from(arr).buffer).toString('utf16le');
    }

    function readUTF8CStr(len, ptr) {
        let arr = [];
        const mem = new Uint8Array(ctx.memory.buffer, Number(ptr));
        for (let i = 0; i < len && mem[i] != 0; i++) {
            arr.push(mem[i]);
        }
        return Buffer.from(arr).toString('utf8');
    }

    function storageRead(key_len, key_ptr) {
        const storageKey = Buffer.from(new Uint8Array(ctx.memory.buffer, Number(key_ptr), Number(key_len)));
        const compKey = dataKey(ctx.contractId, storageKey);
        debug('storage_read', ctx.contractId, prettyBuffer(storageKey));

        ctx.parentPort.postMessage({
            methodName: 'storage_read',
            compKey
        });

        let resultMessage
        do {
            resultMessage = ctx.receiveMessageOnPort(ctx.parentPort);
        } while (!resultMessage);
        return resultMessage.message;
    }


    return {
        // NOTE: See https://github.com/near/nearcore/blob/master/runtime/near-vm-runner/src/logic/logic.rs
        register_len: (register_id) => {
            debug('register_len', register_id);
            if (registers[register_id]) {
                debug('registers[register_id].length', registers[register_id].length);
                return BigInt(registers[register_id].length);
            } else {
                debug('register not found, returning MAX_U64');
                return BigInt(MAX_U64);
            }
        },
        read_register: (register_id, ptr) => {
            debug('read_register', register_id, ptr);
            debug('registers[register_id]', registers[register_id]);
            const mem = new Uint8Array(ctx.memory.buffer)
            mem.set(registers[register_id] || Buffer.from([]), Number(ptr));
        },
        current_account_id: (register_id) => {
            registers[register_id] = Buffer.from(ctx.contractId);
        },
        signer_account_id: prohibitedInView('signer_account_id'),
        signer_account_pk: prohibitedInView('signer_account_pk'),
        predecessor_account_id: prohibitedInView('predecessor_account_id'),
        input: (register_id) => {
            debug('input', register_id);
            registers[register_id] = Buffer.from(ctx.methodArgs);
        },
        block_index: () => {
            return BigInt(ctx.blockHeight);
        },
        block_timestamp: () => {
            return BigInt(ctx.blockTimestamp);
        },
        epoch_height: notImplemented('epoch_height'),
        storage_usage: notImplemented('storage_usage'),

        account_balance: notImplemented('account_balance'), // TODO: Implement as needed for IDO usage
        account_locked_balance: notImplemented('account_locked_balance'),
        attached_deposit: prohibitedInView('attached_deposit'),
        prepaid_gas: prohibitedInView('prepaid_gas'),
        used_gas: prohibitedInView('used_gas'),

        random_seed: notImplemented('random_seed'),
        sha256: (value_len, value_ptr, register_id) => {
            const value = new Uint8Array(ctx.memory.buffer, Number(value_ptr), Number(value_len));
            const hash = createHash('sha256');
            hash.update(value);
            registers[register_id] = hash.digest();
        },
        keccak256: notImplemented('keccak256'),
        keccak512: notImplemented('keccak512'),
        ripemd160: notImplemented('ripemd160'),
        ecrecover: notImplemented('ecrecover'),
        ed25519_verify: notImplemented('ed25519_verify'),

        value_return: (value_len, value_ptr) => {
            debug('value_return', value_len, value_ptr);
            ctx.result = Buffer.from(new Uint8Array(ctx.memory.buffer, Number(value_ptr), Number(value_len)));
        },
        panic: () => {
            const message = `explicit guest panic`
            debug('panic', message);
            throw new FastNEARError('panic', message);
        },
        panic_utf8: (len, ptr) => {
            const message = readUTF8CStr(len, ptr);
            debug('panic', message);
            throw new FastNEARError('panic', message);
        },
        abort: (msg_ptr, filename_ptr, line, col) => {
            debug('abort', msg_ptr, filename_ptr, line, col);
            if (msg_ptr < 4 || filename_ptr < 4) {
                throw new FastNEARError('abort', 'String encoding is bad UTF-16 sequence.');
            }

            const msg_len = new Uint32Array(ctx.memory.buffer, msg_ptr - 4, 1)[0];
            const filename_len = new Uint32Array(ctx.memory.buffer, filename_ptr - 4, 1)[0];

            const msg = readUTF16CStr(msg_len, msg_ptr);
            const filename = readUTF16CStr(filename_len, filename_ptr);

            if (!msg || !filename) {
                throw new FastNEARError('abort', 'String encoding is bad UTF-16 sequence.');
            }

            const message = `${msg}, filename: "${filename}" line: ${line} col: ${col}`;
            debug('abort message', message);

            ctx.logs.push(`ABORT: ${message}`);

            throw new FastNEARError('abort', message);
        },
        log_utf8: (len, ptr) => {
            const message = readUTF8CStr(len, ptr);
            debug(`log: ${message}`);
            ctx.logs.push(message);
        },
        log_utf16: (len, ptr) => {
            const message = readUTF16CStr(len, ptr);
            debug(`log: ${message}`);
            ctx.logs.push(message);
        },

        promise_create: prohibitedInView('promise_create'),
        promise_then: prohibitedInView('promise_then'),
        promise_and: prohibitedInView('promise_and'),
        promise_batch_create: prohibitedInView('promise_batch_create'),
        promise_batch_then: prohibitedInView('promise_batch_then'),
        promise_batch_action_create_account: prohibitedInView('promise_batch_action_create_account'),
        promise_batch_action_deploy_contract: prohibitedInView('promise_batch_action_deploy_contract'),
        promise_batch_action_function_call: prohibitedInView('promise_batch_action_function_call'),
        promise_batch_action_function_call_weight: prohibitedInView('promise_batch_action_function_call_weight'),
        promise_batch_action_transfer: prohibitedInView('promise_batch_action_transfer'),
        promise_batch_action_stake: prohibitedInView('promise_batch_action_stake'),
        promise_batch_action_add_key_with_full_access: prohibitedInView('promise_batch_action_add_key_with_full_access'),
        promise_batch_action_add_key_with_function_call: prohibitedInView('promise_batch_action_add_key_with_function_call'),
        promise_batch_action_delete_key: prohibitedInView('promise_batch_action_delete_key'),
        promise_batch_action_delete_account: prohibitedInView('promise_batch_action_delete_account'),
        promise_results_count: prohibitedInView('promise_results_count'),
        promise_result: prohibitedInView('promise_result'),
        promise_return: prohibitedInView('promise_return'),

        storage_write: prohibitedInView('storage_write'),
        storage_read: (key_len, key_ptr, register_id) => {
            const result = storageRead(key_len, key_ptr);

            if (result == null) {
                debug('storage_read result: none');
                return 0n;
            }

            registers[register_id] = result;
            debug('storage_read result', prettyBuffer(Buffer.from(result)));
            return 1n;
        },
        storage_remove: prohibitedInView('storage_remove'),
        storage_has_key: (key_len, key_ptr) => {
            const result = storageRead(key_len, key_ptr);

            if (result == null) {
                debug('storage_has_key: false');
                return 0n;
            }

            debug('storage_has_key: true');
            return 1n;
        },
        validator_stake: notImplemented('validator_stake'),
        validator_total_stake: notImplemented('validator_total_stake'),
    }
};

module.exports = imports;