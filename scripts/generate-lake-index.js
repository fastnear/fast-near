
async function decompress(data) {
    const zlib = require('zlib');
    const gunzip = zlib.createGunzip();
    const buffer = await new Promise((resolve, reject) => {
        const chunks = [];
        gunzip.on('data', chunk => chunks.push(chunk));
        gunzip.on('error', reject);
        gunzip.on('end', () => resolve(Buffer.concat(chunks)));
        gunzip.write(data);
        gunzip.end();
    });
    return buffer;
}

async function generateIndex() {
    const fs = require('fs').promises;
    const shards = await fs.readdir('data');
    for (let shard of shards) {
        if (!shard.startsWith('shard-')) {
            continue;
        }
        
        const shardPath = `data/${shard}`;
        // TODO: Does readdir sort the files?
        const files = await fs.readdir(shardPath);
        for (let file of files) {
            const filePath = `${shardPath}/${file}`;
            const blockIndex = parseInt(file.split('.')[0]);
            const decompressed = await decompress(await fs.readFile(filePath));
            const message = JSON.parse(decompressed.toString('utf8'));

            for (let { type, change } of message.stateChanges) {
                const parts = type.split('_');
                parts.pop();
                const scope = parts.join('_');
                const key = `${scope}::${change.accountId}::${change.keyBase64 || change.publicKey || ''}`;
                console.log(key, blockIndex);
            }
        }
    }
}

generateIndex().catch(console.error);