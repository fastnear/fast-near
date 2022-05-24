const storageClient = require('./storage-client');
const { FastNEARError } = require('./error');

const START_BLOCK_HEIGHT = process.env.FAST_NEAR_START_BLOCK_HEIGHT || '0';

async function resolveBlockHeight(blockHeight) {
    const latestBlockHeight = await storageClient.getLatestBlockHeight();
    blockHeight = blockHeight || latestBlockHeight;
    if (parseInt(blockHeight, 10) > parseInt(latestBlockHeight, 10)) {
        throw new FastNEARError('blockHeightTooHigh', `Block height not found: ${blockHeight}`);
    }

    if (parseInt(blockHeight, 10) < parseInt(START_BLOCK_HEIGHT, 10)) {
        throw new FastNEARError('blockHeightTooLow', `Block height not found: ${blockHeight}`);
    }
    console.log(`Block height ${blockHeight}`);

    return blockHeight;
}

module.exports = resolveBlockHeight;