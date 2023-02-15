const storage = require("../storage");

async function compressHistory() {
    const blockHeight = await storage.getLatestBlockHeight();
    let iterator;
    do {
        const [newIterator, keys] = await storage.scanAllKeys(iterator); 
        await storage.writeBatch(async batch => {
            for (const key of keys) {
                console.log('compress', JSON.stringify(key.toString('utf8')));
                await storage.cleanOlderData(batch, key, blockHeight);
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