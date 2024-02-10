class LakeStorage {

    getLatestBlockHeight() {
        // TODO
        notImplemented();
    }

    setLatestBlockHeight(blockHeight) {
        readOnly();
    }

    getBlockTimestamp(blockHeight) {
        // TODO
        notImplemented();
    }

    setBlockTimestamp(blockHeight, timestamp) {
        readOnly();
    }

    getLatestDataBlockHeight(compKey, blockHeight) {
        const { accountId, key } = parseCompKey(compKey);

        // TODO
        notImplemented();
    }

    getData(compKey, blockHeight) {
        // TODO
        notImplemented();
    }

    getLatestData(compKey, blockHeight) {
        // TODO
        notImplemented();
    }

    setData(batch, scope, accountId, storageKey, blockHeight, data) {
        readOnly();
    }

    deleteData(batch, scope, accountId, storageKey, blockHeight) {
        readOnly();
    }

    getBlob(hash) {
        // TODO
        notImplemented();
    }

    setBlob(batch, data) {
        readOnly();
    }

    cleanOlderData(batch, key, blockHeight) {
        readOnly();
    }

    scanAllKeys(iterator) {
        // TODO
        notImplemented();
    }

    scanDataKeys(accountId, blockHeight, keyPattern, iterator, limit) {
        // TODO
        notImplemented();
    }

    writeBatch(batch) {
        readOnly();
    }

    clearDatabase() {
        readOnly();
    }

    closeDatabase() {
        // Do nothing
        // TODO: If there is in memory cache, clear it?
    }
}

function readOnly() {
    throw new Error('LakeStorage is read only');
}

function notImplemented() {
    throw new Error('Not implemented');
}

function parseCompKey(compKey) {
    const type = compKey.subarray(0, 1);
    const offset = compKey.indexOf(':', 2);
    const accountId = compKey.toString('utf8', 2, offset);
    const key = Buffer.concat([type, compKey.slice(offset + 1)]);
    return { accountId, key };
}