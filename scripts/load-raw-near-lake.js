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

async function* blockNumbersStream(client, bucketName, startAfter, limit, pageSize = 250) {
    let listObjectsResult;
    const endAt = startAfter + limit;
    do {
        listObjectsResult = await listObjects(client, { bucketName, startAfter: normalizeBlockHeight(startAfter), maxKeys: pageSize });
        const blockNumbers = (listObjectsResult.CommonPrefixes || []).map((p) => parseInt(p.Prefix.split('/')[0]));

        for (const blockNumber of blockNumbers) {
            if (parseInt(blockNumber, 10) >= endAt) {
                return;
            }

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

async function *chunkBlockNumbers(blockNumbers, chunkSize = 5) {
    // Iterate through block numbers and group them by block number rounded to chunkSize
    let lastBlockNumber = null;
    let items = [];

    for await (const blockNumber of blockNumbers) {
        const blockNumberRounded = Math.floor(blockNumber / chunkSize) * chunkSize;
        if (lastBlockNumber === null) {
            lastBlockNumber = blockNumberRounded;
        }

        if (blockNumberRounded > lastBlockNumber) {
            yield items;
            lastBlockNumber = blockNumberRounded;
            items = [];
        }

        items.push(blockNumber);

    }

    if (items.length > 0) {
        yield items;
    }
}

async function withTimeMeasure(name, fn, timeout = 15000) {
    // console.time(name);
    const startTime = Date.now();
    const interval = setInterval(() => {
        console.warn(`${name} took more than ${Date.now() - startTime}ms`);
    }, timeout);

    try {
        return await fn();
    } finally {
        clearInterval(interval);
        // console.timeEnd(name);
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
    let getFileCount = 0;

    const writeQueue = [];
    const MAX_WRITE_QUEUE = 128;

    for await (const blockNumbers of chunkBlockNumbers(blockNumbersStream(client, bucketName, startAfter, limit), FILES_PER_ARCHIVE)) {
        console.log('writeQueue', writeQueue.length, 'getFileCount', getFileCount);

        if (writeQueue.length >= MAX_WRITE_QUEUE) {
            await withTimeMeasure('await writeQueue', async () => {
                await writeQueue.shift();
            });
        }

        const blockNumber = Math.floor(blockNumbers[0] / FILES_PER_ARCHIVE) * FILES_PER_ARCHIVE;
        const task = withTimeMeasure(`task ${blockNumber}`, async () => {
            const blockHeight = normalizeBlockHeight(blockNumber);
            const [prefix1, prefix2] = blockHeight.match(/^(.{6})(.{3})/).slice(1);

            async function writeArchive(folder, entries) {
                const outFolder = `${dstDir}/${folder}/${prefix1}/${prefix2}`;
                const outPath = `${outFolder}/${normalizeBlockHeight(blockHeight)}.tgz`;
                // console.time(outPath);
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
                // console.timeEnd(outPath);
            }

            async function getFile(fileName, blockNumber) {
                getFileCount++;
                try {
                    const blockHeight = normalizeBlockHeight(blockNumber);
                    const blockResponse = await getObject(client, { bucketName, key: `${blockHeight}/${fileName}` });
                    const data = await asBuffer(blockResponse.Body);
                    return { data, blockHeight };
                } finally {
                    getFileCount--;
                    // console.log(`getFileCount: ${getFileCount}`);
                }
            }

            const blocks = await withTimeMeasure(`${blockNumber} block`, async () => {
                // const blocks = await Promise.all(blockNumbers.map((blockNumber) => getFile('block.json', blockNumber)));
                const blocks = [];
                for (let blockNumber of blockNumbers) {
                    blocks.push(await getFile('block.json', blockNumber));
                }
                await writeArchive('block', blocks)
                return blocks;
            });

            const maxShards = Math.max(...blocks.map(({ data }) => JSON.parse(data.toString('utf8')).chunks.length));

            for (let i = 0; i < maxShards; i++) {
                await withTimeMeasure(`${blockNumber} shard ${i}`, async () => {
                    // const shardChunks = await Promise.all(blockNumbers.map((blockNumber) => getFile(`shard_${i}.json`, blockNumber)));
                    const shardChunks = [];
                    for (let blockNumber of blockNumbers) {
                        shardChunks.push(await getFile(`shard_${i}.json`, blockNumber));
                    }
                    await writeArchive(i, shardChunks);
                });
            }

            blocksProcessed += blocks.length;
            const timeElapsed = Date.now() - timeStarted;
            const speed = blocksProcessed / (timeElapsed / 1000);
            const eta = Math.round((limit - blocksProcessed) / speed);
            console.log(blockHeight, `Speed: ${speed} blocks/s`, `ETA: ${eta}s`);
        });
        writeQueue.push(task);
    }

    await Promise.all(writeQueue);
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

