const fs = require('fs');
const compressing = require('compressing');
const { pipeline } = require('stream/promises');
const { config } = require('yargs');

const FILES_PER_ARCHIVE = 100;

function normalizeBlockHeight(number) {
    return number.toString().padStart(12, '0');
}

async function compress(bucketName, startAfter, limit) {
    for (let i = 0; i < limit; i += FILES_PER_ARCHIVE) {
        console.log(i);
        const files = [];

        // TODO: Make dynamic
        const numShards = 4;
        for (let folder of ['block', ...Array.from({ length: numShards }, (_, i) => `${i}`)]) {
            const archiveStream = new compressing.tgz.Stream();
            for (let j = 0; j < FILES_PER_ARCHIVE; j++) {
                const inFolder = `./lake-data/${bucketName}/${folder}`;
                archiveStream.addEntry(`${inFolder}/${normalizeBlockHeight(startAfter + i + j)}.json`, { relativePath: `${normalizeBlockHeight(startAfter + i + j)}.json` });
            }
            const outFolder = `./lake-data-compressed/${bucketName}/${folder}`;
            await fs.promises.mkdir(outFolder, { recursive: true });            
            const outPath = `${outFolder}/${normalizeBlockHeight(startAfter + i)}.tgz`;
            const outStream = fs.createWriteStream(outPath);
            await pipeline(archiveStream, outStream);
        }
    }
}

const [, , bucketName, startAfter, limit] = process.argv;
if (!bucketName) {
    console.error('Usage: node scripts/compress-raw-near-lake.js <bucketName> [startAfter] [limit]');
    process.exit(1);
}

compress(bucketName, parseInt(startAfter || "0"), parseInt(limit || "1000"))
    .catch((error) => {
        console.error('Exiting because of error', error);
        process.exit(1);
    });
