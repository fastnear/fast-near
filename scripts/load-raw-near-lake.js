const {
    S3Client,
    ListObjectsV2Command,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');

const fs = require('fs/promises');

// Setup keep-alive agents for AWS
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const { Agent: HttpAgent } = require('http');
const { Agent: HttpsAgent } = require('https');
const httpAgent = new HttpAgent({ keepAlive: true });
const httpsAgent = new HttpsAgent({ keepAlive: true });

// Avoid DNS lookups for every request
const CacheableLookup = require('cacheable-lookup');
const cacheable = new CacheableLookup();
cacheable.install(httpAgent);
cacheable.install(httpsAgent);

function normalizeBlockHeight(number) {
    return number.toString().padStart(12, '0');
}

async function* blockNumbersStream(client, bucketName, startAfter, pageSize = 50) {
    let listObjects;
    do {
        listObjects = await client.send(
            new ListObjectsV2Command({
                Bucket: bucketName,
                MaxKeys: pageSize,
                Delimiter: '/',
                StartAfter: normalizeBlockHeight(startAfter),
                RequestPayer: 'requester',
            })
        );
        const blockNumbers = (listObjects.CommonPrefixes || []).map((p) => parseInt(p.Prefix.split('/')[0]));

        for (const blockNumber of blockNumbers) {
            yield blockNumber;
        }

        startAfter = blockNumbers[blockNumbers.length - 1] + 1;
    } while (listObjects.IsTruncated);
}

async function* chunkStream(stream, chunkSize) {
    let chunk = [];
    for await (const item of stream) {
        chunk.push(item);
        if (chunk.length >= chunkSize) {
            yield chunk;
            chunk = [];
        }
    }
    if (chunk.length) {
        yield chunk;
    }
}

async function sync(bucketName, startAfter, limit = 1000) {
    const client = new S3Client({
        region: 'eu-central-1',
        requestHandler: new NodeHttpHandler({
            httpAgent,
            httpsAgent,
        }),
    });

    const dstDir = `./lake-data/${bucketName}`;
    const MAX_SHARDS = 4;
    const PAGE_SIZE = 1000;
    const CHUNK_SIZE = 32;

    const endAt = startAfter + limit;
    console.log(`Syncing ${bucketName} from ${startAfter} to ${endAt}`);

    const timeStarted = Date.now();
    let blocksProcessed = 0;

    // mkdir -p necessary folders 
    await fs.mkdir(`${dstDir}/block`, { recursive: true });
    for (let i = 0; i < MAX_SHARDS; i++) {
        await fs.mkdir(`${dstDir}/${i}`, { recursive: true });
    }

    // Iterate blockNumbers stream one chunk at a time
    for await (const blockNumbers of chunkStream(blockNumbersStream(client, bucketName, startAfter, PAGE_SIZE), CHUNK_SIZE)) {
        const filteredBlockNumbers = blockNumbers.filter((blockNumber) => blockNumber < endAt);
        if (!filteredBlockNumbers.length) {
            break;
        }

        await Promise.all(filteredBlockNumbers.map(async (blockNumber) => {
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
    }
}

const [, , bucketName, startAfter, limit] = process.argv;
if (!bucketName) {
    console.error('Usage: node scripts/load-raw-near-lake.js <bucketName> [startAfter] [limit]');
    process.exit(1);
}

sync(bucketName, parseInt(startAfter || "0"), parseInt(limit || "1000"))
    .catch((error) => {
        console.error(error);
        process.exit(1);
    });

