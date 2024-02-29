
async function* transactionStream(blocksStream) {
    for await (const block of blocksStream) {
        console.log('block', block.block.header.height);
        for (const { chunk } of block.shards) {
            if (!chunk) {
                continue;
            }

            for (const transaction of chunk.transactions) {
                yield transaction;
            }
        } 
    }
} 

module.exports = {
    transactionStream
};