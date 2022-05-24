const accountKey = accountId => Buffer.from(`a:${accountId}`);
const dataKey = (accountId, storageKey) => Buffer.concat([Buffer.from(`d:${accountId}:`), storageKey]);
const codeKey = accountId => Buffer.from(`c:${accountId}`);

module.exports = {
    accountKey,
    dataKey,
    codeKey,
}

