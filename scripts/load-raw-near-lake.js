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

async function withRetries(fn, maxAttempts = 3) {
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
        try {
            return await fn();
        } catch (error) {
            console.error(`Attempt ${attempt + 1} failed: `, error);
            if (attempt === maxAttempts - 1) {
                throw error;
            }
        }
    }
}

// TODO: Not sure why AWS doesn't retry as expected

async function listObjects(client, { bucketName, startAfter, maxKeys }) {
    return withRetries(async () => {
        return await client.send(
            new ListObjectsV2Command({
                Bucket: bucketName,
                MaxKeys: maxKeys,
                Delimiter: '/',
                StartAfter: startAfter,
                RequestPayer: 'requester',
            })
        );
    });
}

async function getObject(client, { bucketName, key }) {
    return withRetries(async () => {
        if (Math.random() < 0.01) {
            throw new Error('Random error');
        }

        return await client.send(
            new GetObjectCommand({
                Bucket: bucketName,
                Key: key,
                RequestPayer: 'requester',
            })
        );
    });
}

async function* blockNumbersStream(client, bucketName, startAfter, pageSize = 50) {
    let listObjectsResult;
    do {
        listObjectsResult = await listObjects(client, { bucketName, startAfter: normalizeBlockHeight(startAfter), maxKeys: pageSize });
        const blockNumbers = (listObjectsResult.CommonPrefixes || []).map((p) => parseInt(p.Prefix.split('/')[0]));

        for (const blockNumber of blockNumbers) {
            yield blockNumber;
        }

        startAfter = blockNumbers[blockNumbers.length - 1] + 1;
    } while (listObjectsResult.IsTruncated);
}

async function *blockPromises(client, bucketName, startAfter, limit = 1000) {
    const endAt = startAfter + limit;

    for await (const blockNumber of blockNumbersStream(client, bucketName, startAfter)) {
        if (blockNumber >= endAt) {
            break;
        }

        const promise = (async () => {
            const blockHeight = normalizeBlockHeight(blockNumber);
            const blockData = await getObject(client, { bucketName, key: `${blockHeight}/block.json` });

            const blockReadable = blockData.Body;
            const chunks = [];
            for await (const chunk of blockReadable) {
                chunks.push(chunk);
            }
            const blockBuffer = Buffer.concat(chunks);
            const block = JSON.parse(blockBuffer.toString('utf8'));

            return { block: blockBuffer, blockHeight, shards: await Promise.all(
                block.chunks.map(async (_, i) => {
                    const chunkData = await getObject(client, { bucketName, key: `${blockHeight}/shard_${i}.json` });

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

