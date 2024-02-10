const { BORSH_SCHEMA, PublicKey } = require('./data-model');
const { serialize } = require('borsh');

const ACCOUNT_SCOPE = 'a';
const DATA_SCOPE = 'd';
const ACCESS_KEY_SCOPE = 'k';

// TODO: Check if these still needed, if needed - refactor with _SCOPE constants
const accountKey = accountId => Buffer.from(`a:${accountId}`);
const dataKey = (accountId, storageKey) => Buffer.concat([Buffer.from(`d:${accountId}:`), storageKey]);
const accessKeyKey = (accountId, publicKey) => Buffer.concat([Buffer.from(`k:${accountId}:`), serialize(BORSH_SCHEMA, PublicKey.fromString(publicKey))]);

const allKeysKey = (scope, accountId) => Buffer.from(`k:${scope}:${accountId}`);

const compositeKey = (scope, accountId, storageKey) => {
    const topLevelKey = Buffer.from(`${scope}:${accountId}`);
    return storageKey ? Buffer.concat([topLevelKey, Buffer.from(':'), Buffer.from(storageKey)]) : topLevelKey;
};

module.exports = {
    ACCOUNT_SCOPE,
    DATA_SCOPE,
    ACCESS_KEY_SCOPE,
    accountKey,
    dataKey,
    accessKeyKey,
    allKeysKey,
    compositeKey
}

