
// TODO: Allow to break the loop if the user wants to stop reading blocks. Use an AbortController signal?
async function* readBlocks({ baseUrl = 'https://mainnet.neardata.xyz/v0', startBlockHeight, endBlockHeight, batchSize }) {
    async function fetchBlockNumber(path) {
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

    const fetchBlock = async (blockHeight) => {
        const res = await fetch(`${baseUrl}/block/${blockHeight}`);
        const block = await res.json();
        return block;
    };

    const workPool = [];
    let blockHeight = startBlockHeight;
    let finalBlockHeight = await fetchBlockNumber('last_block/final');
    for (; !endBlockHeight || blockHeight < Math.min(finalBlockHeight, endBlockHeight); blockHeight++) {
        while (workPool.length >= batchSize) {
            const block = await workPool.shift();
            if (block) {
                yield block;
            }
        }

        workPool.push(fetchBlock(blockHeight));

        if (blockHeight % batchSize === 0) {
            finalBlockHeight = await fetchBlockNumber('last_block/final');
        }
    }

    console.log('finishing work');
    while (workPool.length > 0) {
        const block = await workPool.shift();
        if (block) {
            yield block;
        }
    }

    console.log('fetching more');
    for (; !endBlockHeight || blockHeight < endBlockHeight; blockHeight++) {
        const block = await fetchBlock(blockHeight);
        yield block;
    }
}

module.exports = {
    readBlocks
}