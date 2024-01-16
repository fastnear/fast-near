const {
    S3Client,
    ListObjectsV2Command,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');

const compressing = require('compressing');
const fs = require('fs/promises');
const { createWriteStream } = require('fs');
const { pipeline } = require('stream/promises');

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
        const timeKey = `getObject:${bucketName}:${key}`;
        // console.log(timeKey);
        // console.time(timeKey);
        try {
        return await client.send(
            new GetObjectCommand({
                Bucket: bucketName,
                Key: key,
                RequestPayer: 'requester',
            })
        );
        } finally {
            // console.timeEnd(timeKey);
        }
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

async function asBuffer(readable) {
    const chunks = [];
    for await (const chunk of readable) {
        chunks.push(chunk);
    }
    return Buffer.concat(chunks);
}

async function *blockPromises(client, bucketName, startAfter, limit = 1000) {
    const endAt = startAfter + limit;

    for await (const blockNumber of blockNumbersStream(client, bucketName, startAfter)) {
        if (blockNumber >= endAt) {
            break;
        }

        const promise = (async () => {
            const blockHeight = normalizeBlockHeight(blockNumber);
            console.log(blockHeight, 'start');
            console.time(blockHeight);
            try {
            const blockData = await getObject(client, { bucketName, key: `${blockHeight}/block.json` });

            const blockBuffer = await asBuffer(blockData.Body);
            const block = JSON.parse(blockBuffer.toString('utf8'));

            return { block: blockBuffer, blockHeight, shards: await Promise.all(
                block.chunks.map(async (_, i) => {
                    const chunkData = await getObject(client, { bucketName, key: `${blockHeight}/shard_${i}.json` });

                    return chunkData.Body;
                    // return await asBuffer(chunkData.Body);
                }))
            };
        } finally {
            console.timeEnd(blockHeight);
        }
        })();

        // NOTE: Wrapping into object to avoid promise resolution
        yield { promise, blockNumber };
    }
}

async function *chunkByBlockNumber(blockPromises, chunkSize = 5) {
    // Iterate through block promises and group them by block number rounded to chunkSize
    let lastBlockNumber = null;
    let items = [];

    for await (const blockPromise of blockPromises) {
        const blockNumberRounded = Math.floor(blockPromise.blockNumber / chunkSize) * chunkSize;
        if (lastBlockNumber === null) {
            lastBlockNumber = blockNumberRounded;
        }

        if (blockNumberRounded > lastBlockNumber) {
            yield { blockNumber: lastBlockNumber, items };
            lastBlockNumber = blockNumberRounded;
            items = [];
        }

        items.push(blockPromise);
    }

    if (items.length > 0) {
        yield { blockNumber: lastBlockNumber, items };
    }
}

const FILES_PER_ARCHIVE = 5;

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

    const timeStarted = Date.now();
    let blocksProcessed = 0;

    const writeQueue = [];
    const MAX_WRITE_QUEUE = 16;

    for await (const { blockNumber, items } of chunkByBlockNumber(blockPromises(client, bucketName, startAfter, limit), FILES_PER_ARCHIVE)) {
        if (writeQueue.length >= MAX_WRITE_QUEUE) {
            console.time('await writeQueue');
            // await Promise.race(writeQueue);
            await writeQueue.shift();
            console.timeEnd('await writeQueue');
        }

        console.time(`task ${blockNumber}`);
        const task = (async () => {
            const blocks = await Promise.all(items.map(p => p.promise));
            const blockHeight = normalizeBlockHeight(blockNumber);
            const [prefix1, prefix2] = blockHeight.match(/^(.{6})(.{3})/).slice(1);

            async function writeArchive(folder, entries) {
                const outFolder = `${dstDir}/${folder}/${prefix1}/${prefix2}`;
                const outPath = `${outFolder}/${normalizeBlockHeight(blockHeight)}.tgz`;
                console.time(outPath);
                const archiveStream = new compressing.tgz.Stream();
                for (let { data, blockHeight } of entries) {
                    archiveStream.addEntry(data, {
                        size: data?.headers && data.headers['content-length'] ? parseInt(data.headers['content-length']) : undefined,
                        relativePath: `${blockHeight}.json`
                    });
                }

                await fs.mkdir(outFolder, { recursive: true });
                const outStream = createWriteStream(outPath);
                await pipeline(archiveStream, outStream);
                console.timeEnd(outPath);
            }

            await writeArchive('block', blocks.map(({ block, blockHeight }) => ({ data: block, blockHeight })));
            const maxShards = Math.max(...blocks.map(b => b.shards.length));
            await Promise.all(Array.from({ length: maxShards }, (_, i) => writeArchive(i, blocks.map(({ shards, blockHeight }) => ({ data: shards[i], blockHeight })))));

            blocksProcessed += blocks.length;
            // writeQueue.splice(writeQueue.indexOf(task), 1);
            console.log(blockHeight, `Speed: ${blocksProcessed / ((Date.now() - timeStarted) / 1000)} blocks/s`);
            console.timeEnd(`task ${blockNumber}`);
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
        console.error('Exiting because of error', error);
        process.exit(1);
    });

