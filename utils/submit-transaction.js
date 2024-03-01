const { redisBlockStream } = require('./redis-block-stream');
const { transactionStream } = require('./transaction-stream');
const { SignedTransaction, BORSH_SCHEMA } = require('../data-model');
const { deserialize } = require('borsh');
const LRU = require('lru-cache');
const EventEmitter = require('events');
const debug = require('debug')('submit-transaction');

// TODO: Dedupe with json-rpc.js
const NODE_URL = process.env.FAST_NEAR_NODE_URL || 'https://rpc.mainnet.near.org';

async function sendJsonRpc(method, params) {
    const res  = await fetch(`${NODE_URL}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            jsonrpc: '2.0',
            id: 'fast-near',
            method,
            params
        })
    });
    const json = await res.json();
    if (json.error) {
        // TODO: Special RPCError?
        const error = new Error(`RPC Error: ${json.error.message}: ${json.error.data})`);
        error.jsonRpcError = json.error;
        throw error;
    }
    return json.result;
}

async function submitTransactionAsync(transactionData) {
    return await sendJsonRpc('broadcast_tx_async', [transactionData.toString('base64')]);
}

// TODO: Use our own DB instead
async function txStatus(txHash, accountId) {
    return await sendJsonRpc('tx', [txHash, accountId]);
}

let txStream;
const txEventEmitter = new EventEmitter();
const txCache = new LRU({ max: 10000, maxAge: 1000 * 60 * 60 });

async function submitTransaction(transactionData) {
    if (!txStream) {
        const startBlockHeight = (await (await fetch(`${NODE_URL}/status`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            }
        })).json()).sync_info.latest_block_height;
        debug('startBlockHeight:', startBlockHeight);

        const redisUrl = process.env.BLOCKS_REDIS_URL;
        debug('redisUrl:', redisUrl);
        const blocksStream = redisBlockStream({ startBlockHeight, redisUrl, batchSize: 1 });

        txStream = transactionStream(blocksStream);
        await new Promise(async (resolve, reject) => {
            (async () => {
                let streamStarted = false;
                for await (const { transaction, outcome } of txStream) {
                    if (!streamStarted) {
                        streamStarted = true;
                        resolve();
                    }

                    const txHash = transaction.hash;
                    txCache.set(txHash, { transaction, outcome });
                    txEventEmitter.emit('tx', { transaction, outcome });
                }
            })().catch(reject);
        });
    }

    const txHash = await submitTransactionAsync(transactionData);
    debug('Transaction posted:', txHash);
    if (txCache.has(txHash)) {
        return txCache.get(txHash);
    }

    const SUBMIT_TX_STATUS_CHECK_TIMEOUT = 1000 * 10;
    const SUBMIT_TOTAL_TIMEOUT = 1000 * 45;
    const { transaction, outcome } = await new Promise((resolve, reject) => {
        let txCallback;
        const subscribe = () => txEventEmitter.on('tx', txCallback);
        const unsubscribe = () => (txEventEmitter.off('tx', txCallback), txCallback = null);
        setTimeout(() => txCallback && (unsubscribe(), reject(new Error(`Taking more than ${SUBMIT_TOTAL_TIMEOUT}ms to submit transaction`))), SUBMIT_TOTAL_TIMEOUT);

        // NOTE: This is necessary in case transaction already landed before but we don't know
        setTimeout(async function checkStatus() {
            if (!txCallback) return;

            const { transaction: { signerId } } = deserialize(BORSH_SCHEMA, SignedTransaction, transactionData);
            debug('Checking txStatus', txHash, signerId);
            try {
                const result = await txStatus(txHash, signerId);
                const { transaction, transaction_outcome } = result;
                debug('txStatus result:', result);

                if (txCallback) {
                    unsubscribe();
                    resolve({ transaction, outcome: transaction_outcome });
                }
            } catch (error) {
                debug('txStatus error:', error);
                if (error.jsonRpcError) {
                    const { name, cause } = error.jsonRpcError;
                    if (name == 'HANDLER_ERROR' && cause.name ==  'UNKNOWN_TRANSACTION') {
                        setTimeout(checkStatus, SUBMIT_TX_STATUS_CHECK_TIMEOUT);
                        return;
                    }
                    unsubscribe();
                    reject(error);
                }
            }
        }, SUBMIT_TX_STATUS_CHECK_TIMEOUT);

        txCallback = ({ transaction, outcome }) => {
            if (transaction.hash == txHash) {
                unsubscribe();
                resolve({ transaction, outcome });
            }
        };
        subscribe();
    });
    
    const result = {
        // TODO: Other fields
        transaction,
        transaction_outcome: outcome
    }
    console.log('result:', result);
    return result;
}

module.exports = { submitTransaction };