
// TODO: Allow to break the loop if the user wants to stop reading blocks. Use an AbortController signal?
async function* readBlocks({ baseUrl = 'https://mainnet.neardata.xyz/v0', startBlockHeight, endBlockHeight, batchSize }) {
    if (!startBlockHeight) {
        // TODO: Change to use limit parameter as otherwise endBlockHeight is not correct here?
        // Fetch the first block height from the API.
        const res = await fetch(`${baseUrl}/first_block`, { redirect: 'manual' });
        // Parse location header looking like `location: /v0/block/9820210`
        startBlockHeight = parseInt(res.headers.get('location').split('/').pop(), 10);
    }

    const fetchBlock = async (blockHeight) => {
        const res = await fetch(`${baseUrl}/block/${blockHeight}`);
        const block = await res.json();
        return block;
    };

    const workPool = [];
    for (let blockHeight = startBlockHeight; !endBlockHeight || blockHeight < endBlockHeight; blockHeight++) {
        while (workPool.length >= batchSize) {
            const block = await workPool.shift();
            if (block) {
                yield block;
            }
        }

        workPool.push(fetchBlock(blockHeight));
    }
}

module.exports = {
    readBlocks
}