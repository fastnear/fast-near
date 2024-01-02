const storage = require('./storage');
const { FastNEARError } = require('./error');

const START_BLOCK_HEIGHT = process.env.FAST_NEAR_START_BLOCK_HEIGHT || '0';

async function resolveBlockHeight(blockHeight) {
    const latestBlockHeight = await storage.getLatestBlockHeight();
    blockHeight = blockHeight || latestBlockHeight;
    if (parseInt(blockHeight, 10) > parseInt(latestBlockHeight, 10)) {
        throw new FastNEARError('blockHeightTooHigh', `Block height not found: ${blockHeight}`, { blockHeight, latestBlockHeight });
    }

    if (parseInt(blockHeight, 10) < parseInt(START_BLOCK_HEIGHT, 10)) {
        throw new FastNEARError('blockHeightTooLow', `Block height not found: ${blockHeight}`, { blockHeight, startBlockHeight: START_BLOCK_HEIGHT });
    }

    return blockHeight;
}

module.exports = resolveBlockHeight;