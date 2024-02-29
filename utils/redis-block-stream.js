const { createClient } = require('redis');
const { promisify } = require('util');

async function* redisBlockStream({ startBlockHeight, endBlockHeight, redisUrl, streamKey = 'final_blocks', batchSize, abortController }) {
    console.log('redisBlockStream startBlockHeight:', startBlockHeight);
    let redisClient = createClient(redisUrl, {
        detect_buffers: true,
        no_ready_check: true
    });
    // TODO: Does it need to crash as fatal error?
    redisClient.on('error', (err) => console.error('Redis Client Error', err));

    redisClient = {
        end: redisClient.end.bind(redisClient),
        xread: promisify(redisClient.xread).bind(redisClient),
        xrange: promisify(redisClient.xrange).bind(redisClient),
    };

    if (!startBlockHeight) {
        throw new Error('startBlockHeight is required');
    }

    try {
        let blockHeight = startBlockHeight;
        do {
            if (abortController && abortController.signal.aborted) {
                break;
            }

            const result = await redisClient.xread('COUNT', batchSize, 'BLOCK', '100', 'STREAMS', streamKey, blockHeight);
            if (!result) {
                continue;
            }

            const items = result[0][1];
            for (let [id, [, block]] of items) {
                yield JSON.parse(block);
                blockHeight = parseInt(id.split('-')[0]);

                if (endBlockHeight && blockHeight >= endBlockHeight) {
                    return;
                }
            }
        } while (!endBlockHeight || blockHeight < endBlockHeight);
    } finally {
        const flush = true;
        await redisClient.end(flush);
    }
}

module.exports = {
    redisBlockStream
};