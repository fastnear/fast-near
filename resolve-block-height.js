const storageClient = require('./storage-client');
const { FastNEARError } = require('./error');

async function resolveBlockHeight(blockHeight) {
    const latestBlockHeight = await storageClient.getLatestBlockHeight();
    blockHeight = blockHeight || latestBlockHeight;
    if (parseInt(blockHeight, 10) > parseInt(latestBlockHeight, 10)) {
        throw new FastNEARError('blockHeightNotFound', `Block height not found: ${blockHeight}`);
    }

    return blockHeight;
}

module.exports = resolveBlockHeight;