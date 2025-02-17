const { createHash } = require('crypto');
const { keccak_256, keccak_512 } = require('@noble/hashes/sha3');
const { sha512 } = require('@noble/hashes/sha512');
const { ripemd160 } = require('@noble/hashes/ripemd160');
const ed25519 = require('@noble/ed25519');
ed25519.utils.sha512Sync = (...m) => sha512(ed25519.utils.concatBytes(...m));
const secp256k1 = require('@noble/secp256k1');

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
        throw new FastNEARError('prohibitedInView', 'method not available for view calls: ' + name, { methodName: name });
    };

    const registers = ctx.registers;

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

    // NOTE: See https://github.com/near/nearcore/blob/master/runtime/near-vm-runner/src/logic/logic.rs
    // for the original implementation of these methods.
    return {
        // Environment
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
        storage_usage: () => {
            // ctx.storageUsage is in bytes, but the return type is u64
            return new BigUint64Array(ctx.storageUsage.buffer).at(0);
        },

        // Economics
        account_balance: (balance_ptr) => {
            const mem = new Uint8Array(ctx.memory.buffer);
            mem.set(ctx.accountBalance, Number(balance_ptr));
        },
        account_locked_balance: (balance_ptr) => {
            const mem = new Uint8Array(ctx.memory.buffer);
            mem.set(ctx.accountLockedBalance, Number(balance_ptr));
        },

        attached_deposit: prohibitedInView('attached_deposit'),
        prepaid_gas: prohibitedInView('prepaid_gas'),
        used_gas: prohibitedInView('used_gas'),

        // Cryptography
        random_seed: notImplemented('random_seed'),
        sha256: (value_len, value_ptr, register_id) => {
            const value = new Uint8Array(ctx.memory.buffer, Number(value_ptr), Number(value_len));
            const hash = createHash('sha256');
            hash.update(value);
            registers[register_id] = hash.digest();
            debug('sha256', registers[register_id].toString('hex'));
        },
        keccak256: (value_len, value_ptr, register_id) => {
            const value = new Uint8Array(ctx.memory.buffer, Number(value_ptr), Number(value_len));
            const hash = keccak_256(value);
            registers[register_id] = Buffer.from(hash);
            debug('keccak256', registers[register_id].toString('hex'));
        },
        keccak512: (value_len, value_ptr, register_id) => {
            const value = new Uint8Array(ctx.memory.buffer, Number(value_ptr), Number(value_len));
            const hash = keccak_512(value);
            registers[register_id] = Buffer.from(hash);
            debug('keccak512', registers[register_id].toString('hex'));
        },
        ripemd160: (value_len, value_ptr, register_id) => {
            const value = new Uint8Array(ctx.memory.buffer, Number(value_ptr), Number(value_len));
            const hash = ripemd160(value);
            registers[register_id] = Buffer.from(hash);
            debug('ripemd160', registers[register_id].toString('hex'));
        },
        ecrecover: (hash_len, hash_ptr, sig_len, sig_ptr, v, malleability_flag, register_id) => {
            debug('ecrecover', hash_len, hash_ptr, sig_len, sig_ptr, v, malleability_flag, register_id);

            if (hash_len !== 32 || sig_len !== 64) {
                debug('ecrecover invalid input lengths');
                return 0n;
            }

            if (v > 3) {
                debug('ecrecover invalid v value');
                return 0n;
            }

            if (malleability_flag !== 0 && malleability_flag !== 1) {
                debug('ecrecover invalid malleability flag');
                return 0n;
            }

            const hash = new Uint8Array(ctx.memory.buffer, Number(hash_ptr), 32);
            const signature = new Uint8Array(ctx.memory.buffer, Number(sig_ptr), 64);

            try {
                if (malleability_flag === 1) {
                    // Check signature values for ECDSA malleability
                    const r = BigInt(`0x${Buffer.from(signature.slice(0, 32)).toString('hex')}`);
                    const s = BigInt(`0x${Buffer.from(signature.slice(32, 64)).toString('hex')}`);

                    // SECP256K1_N and SECP256K1_N_HALF_ONE values
                    const SECP256K1_N = BigInt('0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141');
                    const SECP256K1_N_HALF_ONE = SECP256K1_N / BigInt(2) + BigInt(1);

                    if (r >= SECP256K1_N || s >= SECP256K1_N_HALF_ONE) {
                        debug('ecrecover signature values out of range');
                        return 0n;
                    }
                }

                const publicKey = secp256k1.recoverPublicKey(hash, signature, v);
                if (!publicKey) {
                    return 0n;
                }

                registers[register_id] = Buffer.from(publicKey.buffer, 1, 64);
                debug('ecrecover', registers[register_id].toString('hex'));
                return 1n;
            } catch (error) {
                debug('ecrecover failed', error);
                return 0n;
            }
        },
        ed25519_verify: (signature_len, signature_ptr, message_len, message_ptr, public_key_len, public_key_ptr) => {
            const signature = new Uint8Array(ctx.memory.buffer, Number(signature_ptr), Number(signature_len));
            const message = new Uint8Array(ctx.memory.buffer, Number(message_ptr), Number(message_len));
            const publicKey = new Uint8Array(ctx.memory.buffer, Number(public_key_ptr), Number(public_key_len));

            if (signature.length !== 64 || publicKey.length !== 32) {
                debug('ed25519_verify invalid input lengths');
                return 0n;
            }

            try {
                const isValid = ed25519.sync.verify(signature, message, publicKey);
                debug('ed25519_verify', isValid);
                return BigInt(isValid);
            } catch (error) {
                debug('ed25519_verify failed', error);
                return 0n;
            }
        },

        // Miscellaneous
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

        // Promises
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

        // Storage
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

        // Validator
        validator_stake: notImplemented('validator_stake'),
        validator_total_stake: notImplemented('validator_total_stake'),

        // Registers
        write_register: prohibitedInView('write_register'),
        read_register: (register_id, ptr) => {
            debug('read_register', register_id, ptr);
            debug('registers[register_id]', registers[register_id]);
            const mem = new Uint8Array(ctx.memory.buffer)
            mem.set(registers[register_id] || Buffer.from([]), Number(ptr));
        },
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

        // Alt BN128
        alt_bn128_g1_multiexp: notImplemented('alt_bn128_g1_multiexp'),
        alt_bn128_g1_sum: notImplemented('alt_bn128_g1_sum'),
        alt_bn128_pairing_check: notImplemented('alt_bn128_pairing_check'),

        // BLS12-381
        bls12381_p1_sum: notImplemented('bls12381_p1_sum'),
        bls12381_p2_sum: notImplemented('bls12381_p2_sum'),
        bls12381_g1_multiexp: notImplemented('bls12381_g1_multiexp'),
        bls12381_g2_multiexp: notImplemented('bls12381_g2_multiexp'),
        bls12381_map_fp_to_g1: notImplemented('bls12381_map_fp_to_g1'),
        bls12381_map_fp2_to_g2: notImplemented('bls12381_map_fp2_to_g2'),
        bls12381_pairing_check: notImplemented('bls12381_pairing_check'),
        bls12381_p1_decompress: notImplemented('bls12381_p1_decompress'),
        bls12381_p2_decompress: notImplemented('bls12381_p2_decompress'),

        // Yield
        promise_yield_create: prohibitedInView('promise_yield_create'),
        promise_yield_resume: prohibitedInView('promise_yield_resume'),
    }
};

module.exports = imports;
