const ACCOUNT_SCOPE = 'a';
const DATA_SCOPE = 'd';
const CODE_SCOPE = 'c';
const ACCESS_KEY_SCOPE = 'k';

// TODO: Check if these still needed, if needed - refactor with _SCOPE constants
const accountKey = accountId => Buffer.from(`a:${accountId}`);
const dataKey = (accountId, storageKey) => Buffer.concat([Buffer.from(`d:${accountId}:`), storageKey]);
const codeKey = accountId => Buffer.from(`c:${accountId}`);
const accessKeyKey = (accountId, storageKey) => Buffer.concat([Buffer.from(`k:${accountId}:`), storageKey]);

const allKeysKey = (scope, accountId) => Buffer.from(`k:${scope}:${accountId}`);

const compositeKey = (scope, accountId, storageKey) => {
    const topLevelKey = Buffer.from(`${scope}:${accountId}`);
    return storageKey ? Buffer.concat([topLevelKey, Buffer.from(':'), Buffer.from(storageKey)]) : topLevelKey;
};

module.exports = {
    ACCOUNT_SCOPE,
    DATA_SCOPE,
    CODE_SCOPE,
    ACCESS_KEY_SCOPE,
    accountKey,
    dataKey,
    codeKey,
    accessKeyKey,
    allKeysKey,
    compositeKey
}

