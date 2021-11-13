const { message } = require('statuses');
const {
    parentPort, workerData, receiveMessageOnPort, threadId
} = require('worker_threads');

const debug = require('debug')(`worker:${threadId}`);

const MAX_U64 = 18446744073709551615n;

const notImplemented = (name) => (...args) => {
    console.error('notImplemented', name, 'args', args);
    throw new Error('method not implemented: ' + name);
};

const imports = (ctx) => {
    const registers = {};

    function readUTF16CStr(ptr) {
        let arr = [];
        const mem = new Uint16Array(ctx.memory.buffer);
        ptr = Number(ptr) / 2;
        while (mem[ptr] != 0) {
            arr.push(mem[ptr]);
            ptr++;
        }
        return Buffer.from(Uint16Array.from(arr).buffer).toString('ucs2');
    }

    return {
        env: {
            register_len: (register_id) => {
                return BigInt(registers[register_id] ? registers[register_id].length : MAX_U64);
            },
            read_register: (register_id, ptr) => {
                const mem = new Uint8Array(ctx.memory.buffer)
                mem.set(registers[register_id] || Buffer.from([]), Number(ptr));
            },

            current_account_id: (register_id) => {
                registers[register_id] = Buffer.from(ctx.contract_id);
            },
            signer_account_id: notImplemented('signer_account_id'),
            signer_account_pk: notImplemented('signer_account_pk'),
            predecessor_account_id: (register_id) => {
                registers[register_id] = Buffer.from(ctx.contract_id);
            },
            input: (register_id) => {
                registers[register_id] = Buffer.from(ctx.methodArgs);
            },
            block_index: notImplemented('block_index'),
            block_timestamp: notImplemented('block_timestamp'),
            epoch_height: notImplemented('epoch_height'),
            storage_usage: notImplemented('storage_usage'),

            account_balance: notImplemented('account_balance'), // TODO: Implement as needed for IDO usage
            account_locked_balance: notImplemented('account_locked_balance'),
            attached_deposit: notImplemented('attached_deposit'),
            prepaid_gas: notImplemented('prepaid_gas'),
            used_gas: notImplemented('used_gas'),

            random_seed: notImplemented('random_seed'),
            sha256: notImplemented('sha256'),
            keccak256: notImplemented('keccak256'),
            keccak512: notImplemented('keccak512'),

            value_return: (value_len, value_ptr) => {
                const mem = new Uint8Array(ctx.memory.buffer)
                ctx.result = Buffer.from(mem.slice(Number(value_ptr), Number(value_ptr + value_len)));
            },
            panic: notImplemented('panic'), // TODO: panic and panic_utf8 for Rust?
            panic_utf8: notImplemented('panic_utf8'),
            abort: (msg_ptr, filename_ptr, line, col) => {
                const message = `abort: ${readUTF16CStr(msg_ptr)} ${readUTF16CStr(filename_ptr)}:${line}:${col}`
                debug(message);
                throw new Error(message);
            },
            // TODO: Collect logs
            log_utf8: notImplemented('log_utf8'),
            log_utf16: notImplemented('log_utf16'),

            promise_create: notImplemented('promise_create'),
            promise_then: notImplemented('promise_then'),
            promise_and: notImplemented('promise_and'),
            promise_batch_create: notImplemented('promise_batch_create'),
            promise_batch_then: notImplemented('promise_batch_then'),
            promise_batch_action_create_account: notImplemented('promise_batch_action_create_account'),
            promise_batch_action_deploy_contract: notImplemented('promise_batch_action_deploy_contract'),
            promise_batch_action_function_call: notImplemented('promise_batch_action_function_call'),
            promise_batch_action_transfer: notImplemented('promise_batch_action_transfer'),
            promise_batch_action_stake: notImplemented('promise_batch_action_stake'),
            promise_batch_action_add_key_with_full_access: notImplemented('promise_batch_action_add_key_with_full_access'),
            promise_batch_action_add_key_with_function_call: notImplemented('promise_batch_action_add_key_with_function_call'),
            promise_batch_action_delete_key: notImplemented('promise_batch_action_delete_key'),
            promise_batch_action_delete_account: notImplemented('promise_batch_action_delete_account'),
            promise_results_count: notImplemented('promise_results_count'),
            promise_result: notImplemented('promise_result'),
            promise_return: notImplemented('promise_return'),

            storage_write: notImplemented('storage_write'),
            storage_read: (key_len, key_ptr, register_id) => {
                const storageKey = Buffer.from(new Uint8Array(ctx.memory.buffer, Number(key_ptr), Number(key_len)));
                const compKey = Buffer.concat([Buffer.from(`${ctx.contractId}:`), storageKey]);
                debug('storage_read', ctx.contractId, storageKey.toString('utf8'));

                parentPort.postMessage({
                    methodName: 'storage_read',
                    compKey
                });

                let resultMessage
                do {
                    resultMessage = receiveMessageOnPort(parentPort);
                } while (!resultMessage);
                const result = resultMessage.message;

                if (!result) {
                    debug('storage_read result: none');
                    return 0n;
                }

                registers[register_id] = result;
                debug('storage_read result', Buffer.from(result).toString('utf8'));
                return 1n;
            },
            storage_remove: notImplemented('storage_remove'),
            storage_has_key: notImplemented('storage_has_key'), // TODO: But is it used in a wild?

            validator_stake: notImplemented('validator_stake'),
            validator_total_stake: notImplemented('validator_total_stake'),
        }
    }
};

async function runWASM({ wasmModule, contractId, methodName, methodArgs }) {
    debug('runWASM', contractId, methodName, Buffer.from(methodArgs).toString('utf8'));
    const ctx = {
        contractId,
        methodArgs
    };
    debug('module instantiate');
    const wasm2 = await WebAssembly.instantiate(wasmModule, imports(ctx));
    debug('module instantiate done');
    ctx.memory = wasm2.exports.memory;
    try {
        debug(`run ${methodName}`);
        wasm2.exports[methodName]();
    } finally {
        debug(`run ${methodName} done`);
    }

    return ctx.result;
}

parentPort.on('message', message => {
    if (message.wasmModule) {
        runWASM(message).then(result => {
            parentPort.postMessage({ result });
        }).catch(error => {
            parentPort.postMessage({ error });
        });
    }
});
