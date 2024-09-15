const { FastNEARError } = require('../error');

const debug = require('debug')('source:neardata');

// TODO: Allow to break the loop if the user wants to stop reading blocks. Use an AbortController signal?
async function* readBlocks({ baseUrl = 'https://mainnet.neardata.xyz/v0', startBlockHeight, endBlockHeight, batchSize = 10, retryDelay = 1000 }) {
    debug('readBlocks', baseUrl, startBlockHeight, endBlockHeight, batchSize, retryDelay);

    async function fetchBlockNumber(path) {
        debug('fetchBlockNumber', path);
        const res = await fetch(`${baseUrl}/${path}`, { redirect: 'manual' });
        // TODO: Should handle 404, etc?
        // Parse location header looking like `location: /v0/block/9820210`
        return parseInt(res.headers.get('location').split('/').pop(), 10);
    }

    if (!startBlockHeight) {
        // TODO: Change to use limit parameter as otherwise endBlockHeight is not correct here?
        // Fetch the first block height from the API.
        startBlockHeight = await fetchBlockNumber('first_block');
    }
    debug('startBlockHeight', startBlockHeight);

    const fetchBlock = async (blockHeight) => {
        debug('fetchBlock', blockHeight);
        const MAX_RETRIES = 3;
        const RETRY_DELAY = 5000;

        let retries = 0;
        while (true) {
            try {
                const res = await fetch(`${baseUrl}/block/${blockHeight}`);
                if (!res.ok) {
                    if (!res.headers.get('content-type')?.includes('application/json')) {
                        const text = await res.text();
                        console.error(`Unexpected response format for block ${blockHeight}:`, text);
                        throw new FastNEARError('unexpectedResponseFormat', `Unexpected response format for block ${blockHeight}`, { text, status: res.status });
                    }

                    const data = { ...await res.json(), blockHeight };
                    if (res.status == 404) {
                        throw new FastNEARError('blockNotFound', `Block ${blockHeight} not found`, data);
                    }
                    throw new FastNEARError('fetchError', `Error fetching block ${blockHeight}`, data);
                }

                const block = await res.json();
                // NOTE: Some blocks are null because they are skipped in chain,
                // e.g. https://a0.mainnet.neardata.xyz/v0/block/121967871
                return block;
            } catch (error) {
                // NOTE: Should't retry on some errors
                // TODO: Check other errors
                if (error.code === 'blockNotFound' && error.data?.type === 'BLOCK_HEIGHT_TOO_LOW') {
                    throw error;
                }

                if (retries < MAX_RETRIES) {
                    console.error(`Error fetching block ${blockHeight}. Retrying (${retries + 1}/${MAX_RETRIES}):`, error);
                    await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
                    retries++;
                } else {
                    throw error;
                }
            }
        }
    };

    // TODO: Special API just to fetch one block?
    if (endBlockHeight === startBlockHeight + 1) {
        debug('fetching single block', startBlockHeight);
        const block = await fetchBlock(startBlockHeight);
        if (block) {
            debug('fetched block', block.block.header.height);
            yield block;
        } else {
            debug('skipped block');
        }
        return;
    }

    const workPool = [];
    let blockHeight = startBlockHeight;
    let finalBlockHeight = await fetchBlockNumber('last_block/final');
    debug('finalBlockHeight', finalBlockHeight);
    for (; blockHeight < Math.min(finalBlockHeight, endBlockHeight || Infinity); blockHeight++) {
        while (workPool.length >= batchSize) {
            const block = await workPool.shift();
            if (block) {
                debug('fetched block', block.block.header.height);
                yield block;
            } else {
                debug('skipped block');
            }
        }

        workPool.push(fetchBlock(blockHeight));

        if (blockHeight % batchSize === 0) {
            finalBlockHeight = await fetchBlockNumber('last_block/final');
            debug('finalBlockHeight', finalBlockHeight);
        }
    }

    debug('finishing work', workPool.length);
    while (workPool.length > 0) {
        const block = await workPool.shift();
        if (block) {
            debug('fetched block', block.block.header.height);
            yield block;
        } else {
            debug('skipped block');
        }
    }

    debug('fetching more');
    for (; !endBlockHeight || blockHeight < endBlockHeight; blockHeight++) {
        const block = await fetchBlock(blockHeight);
        if (block) {
            debug('fetched block', block.block.header.height);
            yield block;
        } else {
            debug('skipped block');
        }
    }
}

module.exports = {
    readBlocks
}