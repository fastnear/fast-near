
async function* transactionStream(blocksStream) {
    for await (const block of blocksStream) {
        console.log('block:', block);
        for (const { chunk } of block.shards) {
            for (const transaction of chunk.transactions) {
                yield transaction;
            }
        } 
    }
} 

module.exports = {
    transactionStream
};