const {
    S3Client,
    ListObjectsV2Command,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');
const { fromEnv } = require('@aws-sdk/credential-providers');

// Setup keep-alive agents for AWS
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const { Agent: HttpAgent } = require('http');
const { Agent: HttpsAgent } = require('https');
const httpAgent = new HttpAgent({ keepAlive: true });
const httpsAgent = new HttpsAgent({ keepAlive: true });

async function listObjects(client, { bucketName, startAfter, maxKeys }) {
    return await client.send(
        new ListObjectsV2Command({
            Bucket: bucketName,
            MaxKeys: maxKeys,
            Delimiter: '/',
            StartAfter: startAfter,
            RequestPayer: 'requester',
        })
    );
}

async function getObject(client, { bucketName, key }) {
    return await client.send(
        new GetObjectCommand({
            Bucket: bucketName,
            Key: key,
            RequestPayer: 'requester',
        })
    );
}

function normalizeBlockHeight(number) {
    return number.toString().padStart(12, '0');
}

async function asBuffer(readable) {
    const chunks = [];
    for await (const chunk of readable) {
        chunks.push(chunk);
    }
    return Buffer.concat(chunks);
}

async function* blockNumbersStream(client, bucketName, startBlockHeight, endBlockHeight, pageSize = 250) {
    let listObjectsResult;
    do {
        listObjectsResult = await listObjects(client, { bucketName, startAfter: normalizeBlockHeight(startBlockHeight), maxKeys: pageSize });
        const blockNumbers = (listObjectsResult.CommonPrefixes || []).map((p) => parseInt(p.Prefix.split('/')[0]));

        for (const blockNumber of blockNumbers) {
            if (parseInt(blockNumber, 10) >= endBlockHeight) {
                return;
            }

            yield blockNumber;
        }

        startBlockHeight = parseInt(blockNumbers[blockNumbers.length - 1], 10) + 1;
    } while (listObjectsResult.IsTruncated);
}

// TODO: shards
async function* readBlocks({ bucket, region, endpoint, startBlockHeight, endBlockHeight, batchSize }) {
    console.log('readBlocks', { bucket, region, endpoint, startBlockHeight, endBlockHeight, batchSize });
    const client = new S3Client({
        credentials: fromEnv(),
        region,
        endpoint,
        requestHandler: new NodeHttpHandler({
            httpAgent,
            httpsAgent,
        }),
        maxAttempts: 3,
    });

    const maxRetries = 3;
    const retryTimeout = 1000;

    async function getJson(fileName, blockNumber) {
        const blockHeight = normalizeBlockHeight(blockNumber);
        for (let i = 0; i < maxRetries; i++) {
            try {
                const blockResponse = await getObject(client, { bucketName: bucket, key: `${blockHeight}/${fileName}` });
                const data = await asBuffer(blockResponse.Body);
                return JSON.parse(data.toString());
            } catch (error) {
                console.error(`Error reading ${fileName} for block ${blockNumber}`, error);
                if (['ECONNRESET', 'ENOTFOUND', 'ETIMEDOUT'].includes(error.code)) {
                    // Retry on connection reset
                    console.log(`Retrying block ${blockNumber}, file ${fileName} after timeout. Attempt ${i + 1}`);
                    await sleep(retryTimeout);
                    continue;
                }
                throw error;
            }
        }
        throw new Error(`Retries exceeded for block ${blockNumber}, file ${fileName}`);
    }

    try {
        const workPool = [];

        const PAGE_SIZE = 69;
        for await (const blockNumber of blockNumbersStream(client, bucket, startBlockHeight, endBlockHeight, PAGE_SIZE)) {
            if (workPool.length >= batchSize) {
                yield await workPool.shift();
            }

            workPool.push((async () => {
                const block = await getJson('block.json', blockNumber);
                // TODO: If list of shards is known, we can use that instead of waiting for the block.json
                const shards = await Promise.all(block.chunks.map(async (_, i) => getJson(`shard_${i}.json`, blockNumber)));
                return { block, shards };
            })());
        }

        for (const work of workPool) {
            yield await work;
        }
    } finally {
        client.destroy();
    }
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = { readBlocks };