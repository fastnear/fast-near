const test = require('tape');
const fs = require('fs/promises');

const { writeChangesFile, readChangesFile, mergeChangesFiles } = require('../storage/lake/changes-index');

const ROOT_DIR = 'test/data/lake/index';

const roundtrip = customTest((test, fileName) => {
    test(`roundtrip ${fileName}`, async t => {
        const changes = await readChanges(fileName);
        const tempFileName = `${fileName}.tmp`;
        await writeChangesFile(tempFileName, convertChanges(changes));
        const tempChanges = await readChanges(tempFileName);
        t.deepEqual(tempChanges, changes);
        t.ok((await fs.readFile(`${ROOT_DIR}/${tempFileName}`)).equals(await fs.readFile(`${ROOT_DIR}/${fileName}`)));
    });
});

roundtrip('app.nearcrowd.near.dat');
roundtrip('asset-manager.orderly-network.near.dat');
roundtrip('changes.dat');

const indexLookup = customTest((test, fileName, options, validateFn) => {
    test(`test filter ${fileName} ${options.accountId} ${options.keyPrefix} ${options.blockHeight}`, async t => {
        const changes = await readChanges(fileName, options);
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

indexLookup('changes.dat', { accountId: '00eisrqqfpdj.users.kaiching', keyPrefix: Buffer.from('6B00', 'hex') }, (t, changes) => {
    t.equals(changes.length, 1);
    t.equals(changes[0].accountId, '00eisrqqfpdj.users.kaiching');
    t.equals(changes[0].key.toString('hex'), '6b00197e30ace8b1e60a9501f8a26d67168be2320fe3b1ff92f7d03fa1fe2d434677');
    t.deepEqual(changes[0].changes, [110012886]);
});

indexLookup('app.nearcrowd.near.dat', { accountId: 'app.nearcrowd.near', keyPrefix: Buffer.from('6474', 'hex') }, (t, changes) => {
    t.equals(changes.length, 6692);
    t.equals(changes[0].accountId, 'app.nearcrowd.near');
    t.equals(changes[0].key.toString('hex'), '6474010000006200acd60000000000');
    t.deepEqual(changes[0].changes, [110012583]);
});

indexLookup('app.nearcrowd.near.dat', { accountId: 'app.nearcrowd.near', keyPrefix: Buffer.from('6470', 'hex'), blockHeight: 110012395 }, (t, changes) => {
    t.equals(changes.length, 2);
    t.equals(changes[0].accountId, 'app.nearcrowd.near');
    t.equals(changes[0].key.toString('hex'), '647001000000');
    t.deepEqual(changes[0].changes, [110012395, 110012391]);
});

// TODO: Force finding page in the middle of index

test('trivial merge', async t => {
    await mergeChangesFiles(
        `${ROOT_DIR}/merged.dat.tmp`, [
            `${ROOT_DIR}/app.nearcrowd.near.dat`,
            `${ROOT_DIR}/asset-manager.orderly-network.near.dat`
        ]);
    const mergedData = await fs.readFile(`${ROOT_DIR}/merged.dat.tmp`); 
    t.ok(mergedData.length > 0);
    await fs.unlink(`${ROOT_DIR}/merged.dat.tmp`);
     
    // merge in opposite order should give same result
    await mergeChangesFiles(
        `${ROOT_DIR}/merged.dat.tmp`, [
            `${ROOT_DIR}/asset-manager.orderly-network.near.dat`,
            `${ROOT_DIR}/app.nearcrowd.near.dat`
        ]);
    const mergedData2 = await fs.readFile(`${ROOT_DIR}/merged.dat.tmp`);
    t.ok(mergedData2.length > 0);
    t.ok(mergedData.equals(mergedData2));

    // verify that merge has all data
    const mergedChanges = await readChanges('merged.dat.tmp');
    const appChanges = await readChanges('app.nearcrowd.near.dat');
    const assetChanges = await readChanges('asset-manager.orderly-network.near.dat');
    t.equals(mergedChanges.length, appChanges.length + assetChanges.length);
    const mergedStrings = mergedChanges.map(changesAsString);
    const appStrings = appChanges.map(changesAsString);
    const assetStrings = assetChanges.map(changesAsString);
    t.deepEqual(mergedStrings, appStrings.concat(assetStrings));
    // very all data sorted
    t.deepEqual([...mergedStrings].sort(), mergedStrings);
    t.deepEqual([...appStrings].sort(), appStrings);
    t.deepEqual([...assetStrings].sort(), assetStrings);
});

async function readChanges(fileName, options) {
    return await readStream(await readChangesFile(`${ROOT_DIR}/${fileName}`, options));
}

function changesAsString({ accountId, key, changes }) {
    return `${accountId} ${key.toString('hex')} ${10_000_000_000 - changes[0]}`;
}

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