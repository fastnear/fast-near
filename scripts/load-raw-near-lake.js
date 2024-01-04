const {
    S3Client,
    ListObjectsV2Command,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');

const fs = require('fs/promises');

function normalizeBlockHeight(number) {
    return number.toString().padStart(12, '0');
}

async function sync(bucketName, startAfter, limit = 1000) {
    const client = new S3Client({
        region: 'eu-central-1',
    });

    const dstDir = `./lake-data/${bucketName}`;
    const MAX_SHARDS = 4;

    const timeStarted = Date.now();
    let blocksProcessed = 0;

    // mkdir -p necessary folders 
    await fs.mkdir(`${dstDir}/block`, { recursive: true });
    for (let i = 0; i < MAX_SHARDS; i++) {
        await fs.mkdir(`${dstDir}/${i}`, { recursive: true });
    }

    let listObjects;
    do {
        listObjects = await client.send(
            new ListObjectsV2Command({
                Bucket: bucketName,
                MaxKeys: limit,
                Delimiter: '/',
                StartAfter: normalizeBlockHeight(startAfter),
                RequestPayer: 'requester',
            })
        );
        const blockNumbers = (listObjects.CommonPrefixes || []).map((p) => parseInt(p.Prefix.split('/')[0]));

        await Promise.all(blockNumbers.map(async (blockNumber) => {
            const blockHeight = normalizeBlockHeight(blockNumber);
            const blockData = await client.send(
                new GetObjectCommand({
                    Bucket: bucketName,
                    Key: `${blockHeight}/block.json`,
                    RequestPayer: 'requester',
                })
            );

            const blockReadable = blockData.Body;
            const chunks = [];
            for await (const chunk of blockReadable) {
                chunks.push(chunk);
            }
            const blockBuffer = Buffer.concat(chunks);
            await fs.writeFile(`${dstDir}/block/${blockHeight}.json`, blockBuffer);

            const block = JSON.parse(blockBuffer.toString('utf8'));
            console.log(block.header.height, block.header.hash, block.chunks.length, `Speed: ${blocksProcessed / ((Date.now() - timeStarted) / 1000)} blocks/s`);

            await Promise.all(block.chunks.map(async (_, i) => {
                const chunkData = await client.send(
                    new GetObjectCommand({
                        Bucket: bucketName,
                        Key: `${blockHeight}/shard_${i}.json`,
                        RequestPayer: 'requester',
                    })
                );

                const chunkReadable = chunkData.Body;

                await fs.writeFile(`${dstDir}/${i}/${blockHeight}.json`, chunkReadable);
            }));

            blocksProcessed++;
        }));

        startAfter = blockNumbers[blockNumbers.length - 1] + 1;
    } while (listObjects.IsTruncated);
}

sync('near-lake-data-mainnet', 100_000_000, 50)
    .catch((error) => {
        console.error(error);
        process.exit(1);
    });

