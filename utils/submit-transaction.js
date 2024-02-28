const { redisBlockStream } = require('./redis-block-stream');
const { transactionStream } = require('./transaction-stream');

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

async function submitTransaction(transactionData) {
    // TODO: Just save latest one
    const startBlockHeight = (await (await fetch(`${NODE_URL}/status`, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json'
        }
    })).json()).sync_info.latest_block_height;
    console.log('startBlockHeight:', startBlockHeight);

    const txHash = await submitTransactionAsync(transactionData);
    console.log('Transaction posted:', txHash);

    const redisUrl = process.env.BLOCKS_REDIS_URL;
    const blocksStream = redisBlockStream({ startBlockHeight, redisUrl, batchSize: 1 });
    for await (const { transaction } of transactionStream(blocksStream)) {
        if (transaction.hash === txHash) {
            console.log('Transaction found:', transaction);
            // TODO: Return proper result
            return transaction;
        }
    }
    console.log('WAT');
    // TODO: Abort stream at some point?
}

module.exports = { submitTransaction };