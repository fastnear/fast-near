const test = require('tape');
const fs = require('fs/promises');

const { writeChangesFile, readChangesFile, mergeChangesFiles } = require('../storage/lake/changes-index');

function roundtripTest(test, fileName) {
    test(`roundtrip ${fileName}`, async t => {
        const indexFileName = `test/data/lake/index/${fileName}`;
        const changes = await readStream(await readChangesFile(indexFileName));
        const tempFileName = `${indexFileName}.tmp`;
        await writeChangesFile(tempFileName, convertChanges(changes));
        const tempChanges = await readStream(await readChangesFile(tempFileName));
        t.deepEqual(tempChanges, changes);
        t.ok((await fs.readFile(tempFileName)).equals(await fs.readFile(indexFileName)));
    });
}
function roundtrip(fileName) {
    roundtripTest(test, fileName);
}
roundtrip.only = function(fileName) {
    roundtripTest(test.only, fileName);
};

roundtrip('app.nearcrowd.near.dat');
roundtrip('asset-manager.orderly-network.near.dat');
roundtrip('changes.dat');


async function readStream(stream) {
    const chunks = [];
    for await (const chunk of stream) {
        chunks.push(chunk);
    }
    return chunks;
}

function convertChanges(allChanges) {
    const result = {};
    for (const { accountId, key, changes } of allChanges) {
        result[accountId] = result[accountId] || [];
        result[accountId].push({ key, changes });
    }
    return result;
}