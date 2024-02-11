const fs = require('fs/promises');
const { readChangesFile } = require('./lake/changes-index');
const { readBlocks } = require('./lake/archive');

class LakeStorage {

    dataDir = './lake-data/near-lake-data-mainnet';

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

    async getLatestDataBlockHeight(compKey, blockHeight) {
        const { accountId, key } = parseCompKey(compKey);
        const shard = shardForAccount(accountId);

        let indexFile = `${this.dataDir}/${shard}/${accountId}.dat`;
        // check if the file exists
        if (!await fs.access(indexFile).catch(() => null)) {
            indexFile = `${this.dataDir}/${shard}/changes.dat`;
        }

        const changesStream = readChangesFile(indexFile, { accountId, keyPrefix: key });
        for await (const { key: k, changes } of changesStream) {
            if (k.equals(key)) {
                return changes.findLast(bh => bh <= blockHeight);
            }
        }
    }

    getData(compKey, blockHeight) {
        const { accountId, key } = parseCompKey(compKey);
        const shard = shardForAccount(accountId);

        for await (const { data, blockHeight } of readBlocks(this.dataDir, shard, blockHeight, blockHeight)) {
            if (blockHeight !== blockHeight) {
                continue;
            }

            const { state_changes, chunk } = JSON.parse(data.toString('utf-8'));
            if (!chunk) {
                continue;
            }

            for (let { type, change } of state_changes) {
                const { account_id, ...changeData } = change;
                if (account_id !== accountId) {
                    continue;
                }

                const k = changeKey(type, changeData);
                if (k.equals(key)) {
                    console.log('found data', changeData);
                    return changeData;
                }
            }
        }

        return null;
    }

    getLatestData(compKey, blockHeight) {
        const dataBlockHeight = this.getLatestDataBlockHeight(compKey, blockHeight);
        return dataBlockHeight && this.getData(compKey, dataBlockHeight);
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
    return { accountId, key, type };
}

function shardForAccount(accountId) {
    // TODO: Don't hardcode this
    // NOTE: This needs to match nearcore logic here: https://github.com/near/nearcore/blob/c6afdd71005a0f9b3e57244188ca02b97eeb0395/core/primitives/src/shard_layout.rs#L239
    const boundaryAccounts = ["aurora", "aurora-0", "kkuuue2akv_1630967379.near"];
    return boundaryAccounts.findIndex(boundaryAccount => accountId < boundaryAccount);
}