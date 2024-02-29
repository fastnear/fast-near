const { redisBlockStream } = require('./redis-block-stream');
const { transactionStream } = require('./transaction-stream');
const LRU = require('lru-cache');
const EventEmitter = require('events');

// TODO: Dedupe with json-rpc.js
const NODE_URL = process.env.FAST_NEAR_NODE_URL || 'https://rpc.mainnet.near.org';

async function submitTransactionAsync(transactionData) {
    const res  = await fetch(`${NODE_URL}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            jsonrpc: '2.0',
            id: 'fast-near',
            method: 'broadcast_tx_async',
            params: [transactionData.toString('base64')]
        })
    });
    const json = await res.json();
    return json.result;
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
        console.log('startBlockHeight:', startBlockHeight);

        const redisUrl = process.env.BLOCKS_REDIS_URL;
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
    console.log('Transaction posted:', txHash);
    if (txCache.has(txHash)) {
        return txCache.get(txHash);
    }

    // TODO: Need to timeout
    const { transaction, outcome } = await new Promise((resolve) => {
        const cb = ({ transaction, outcome }) => {
            if (transaction.hash == txHash) {
                txEventEmitter.off('tx', cb);
                resolve({ transaction, outcome });
            }
        };
        txEventEmitter.on('tx', cb);
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