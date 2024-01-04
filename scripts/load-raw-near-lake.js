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

async function *blockPromises(client, bucketName, startAfter, limit = 1000) {
    const endAt = startAfter + limit;

    for await (const blockNumber of blockNumbersStream(client, bucketName, startAfter)) {
        if (blockNumber >= endAt) {
            break;
        }

        const promise = (async () => {
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
            const block = JSON.parse(blockBuffer.toString('utf8'));

            return { block: blockBuffer, blockHeight, shards: await Promise.all(
                block.chunks.map(async (_, i) => {
                    const chunkData = await client.send(
                        new GetObjectCommand({
                            Bucket: bucketName,
                            Key: `${blockHeight}/shard_${i}.json`,
                            RequestPayer: 'requester',
                        })
                    );

                    return chunkData.Body;
                }))
            };
        })();

        // NOTE: Wrapping into object to avoid promise resolution
        yield { promise, blockNumber };
    }
}

async function sync(bucketName, startAfter, limit = 1000) {
    const client = new S3Client({
        region: 'eu-central-1',
        requestHandler: new NodeHttpHandler({
            httpAgent,
            httpsAgent,
        }),
        maxAttempts: 3,
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

    const writeQueue = [];
    const MAX_WRITE_QUEUE = 32;

    for await (const blockPromise of blockPromises(client, bucketName, startAfter, limit)) {
        if (writeQueue.length >= MAX_WRITE_QUEUE) {
            await Promise.race(writeQueue);
        }

        const task = (async () => {
            const { block, blockHeight, shards } = await blockPromise.promise;

            await fs.writeFile(`${dstDir}/block/${blockHeight}.json`, block);
            for (let i = 0; i < shards.length; i++) {
                await fs.writeFile(`${dstDir}/${i}/${blockHeight}.json`, shards[i]);
            }

            blocksProcessed++;
            writeQueue.splice(writeQueue.indexOf(task), 1);
            console.log(blockHeight, `Speed: ${blocksProcessed / ((Date.now() - timeStarted) / 1000)} blocks/s`);
        })();
        writeQueue.push(task);
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

