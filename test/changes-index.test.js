const test = require('tape');
const fs = require('fs/promises');

const { writeChangesFile, readChangesFile, mergeChangesFiles } = require('../storage/lake/changes-index');

const roundtrip = customTest((test, fileName) => {
    test(`roundtrip ${fileName}`, async t => {
        const indexFileName = `test/data/lake/index/${fileName}`;
        const changes = await readStream(await readChangesFile(indexFileName));
        const tempFileName = `${indexFileName}.tmp`;
        await writeChangesFile(tempFileName, convertChanges(changes));
        const tempChanges = await readStream(await readChangesFile(tempFileName));
        t.deepEqual(tempChanges, changes);
        t.ok((await fs.readFile(tempFileName)).equals(await fs.readFile(indexFileName)));
    });
});

roundtrip('app.nearcrowd.near.dat');
roundtrip('asset-manager.orderly-network.near.dat');
roundtrip('changes.dat');

const indexLookup = customTest((test, fileName, options, validateFn) => {
    test(`test filter ${fileName} ${options.accountId} ${options.keyPrefix} ${options.blockHeight}`, async t => {
        const indexFileName = `test/data/lake/index/${fileName}`;
        const changes = await readStream(await readChangesFile(indexFileName, options));
        validateFn(t, changes);
    });
});

indexLookup('changes.dat', { accountId: '1bc0252107b4d6d0e797d371a9f1f0ffc6026da2f040a97f15eccc2f5dbe1ab2' }, (t, changes) => {
    t.equals(changes.length, 2);
    t.equals(changes[0].accountId, '1bc0252107b4d6d0e797d371a9f1f0ffc6026da2f040a97f15eccc2f5dbe1ab2');
    t.equals(changes[0].key.toString('hex'), '61');
    t.deepEqual(changes[0].changes, [110012726, 110012724]);
    t.equals(changes[1].accountId, '1bc0252107b4d6d0e797d371a9f1f0ffc6026da2f040a97f15eccc2f5dbe1ab2');
    t.equals(changes[1].key.toString('hex'), '6b001bc0252107b4d6d0e797d371a9f1f0ffc6026da2f040a97f15eccc2f5dbe1ab2');
    t.deepEqual(changes[1].changes, [110012724]);
});

function customTest(fn) {
    const result = function(...args) {
        fn(test, ...args);
    }
    result.only = function(...args) {
        fn(test.only, ...args);
    }
    return result;
}

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