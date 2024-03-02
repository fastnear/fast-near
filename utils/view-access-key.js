const { FastNEARError } = require('../error');
const storage = require('../storage');
const { accessKeyKey } = require('../storage-keys');
const { BORSH_SCHEMA, AccessKey, } = require('../data-model');
const { deserialize } = require('borsh');

async function viewAccessKey({ accountId, publicKey, blockHeight }) {
    const storageKey = accessKeyKey(accountId, publicKey);
    const data = await storage.getLatestData(storageKey, blockHeight);
    if (!data) {
        return null;
    }

    const { nonce, permission: { functionCall, fullAccess } } = deserialize(BORSH_SCHEMA, AccessKey, data);
    let permission;
    if (functionCall) {
        const { allowance, receiverId, methodNames } = functionCall;
        permission = {
            type: 'FunctionCall',
            method_names: methodNames,
            receiver_id: receiverId,
            allowance: allowance.toString(10)
        }
    } else if (fullAccess) {
        permission = {
            type: 'FullAccess'
        }
    } else {
        throw FastNEARError('unexpectedPermissionType', 'unexpected permission type');
    }

    return {
        public_key: publicKey,
        nonce: nonce.toString(),
        ...permission
    };
}

module.exports = {
    viewAccessKey
};
