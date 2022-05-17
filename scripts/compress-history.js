const { getLatestBlockHeight, cleanOlderData, scanAllKeys } = require("../storage-client");

async function compressHistory() {
    const blockHeight = await getLatestBlockHeight();
    let iterator;
    do {
        const [newIterator, keys] = await scanAllKeys(iterator); 
        await Promise.all(keys.map(async key => {
            console.log('compress', JSON.stringify(key.toString('utf8')));
            await cleanOlderData(key, blockHeight);
        }));
        iterator = newIterator;
    } while (iterator.toString('utf8') != '0');
}

module.exports = compressHistory;

if (!module.parent) {
    compressHistory().catch(e => {
        console.error(e);
        process.exit(1);
    }).then(() => {
        process.exit(0);
    });
}