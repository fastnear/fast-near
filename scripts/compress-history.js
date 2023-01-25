const storageClient = require("../storage-client");

async function compressHistory() {
    const blockHeight = await storageClient.getLatestBlockHeight();
    let iterator;
    do {
        const [newIterator, keys] = await storageClient.scanAllKeys(iterator); 
        await storageClient.writeBatch(async batch => {
            for (const key of keys) {
                console.log('compress', JSON.stringify(key.toString('utf8')));
                await storageClient.cleanOlderData(batch, key, blockHeight);
            }
        });
        iterator = newIterator;
    } while (iterator.toString('utf8') != '0');
}

module.exports = compressHistory;

if (require.main === module) {
    compressHistory().catch(e => {
        console.error(e);
        process.exit(1);
    }).then(() => {
        process.exit(0);
    });
}