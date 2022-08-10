const assert = require('assert');
const bs58 = require('bs58');

class BaseMessage {
    constructor(args) {
        Object.assign(this, args);
    }
}

class Enum {
    constructor(args) {
        assert(Object.keys(args).length == 1, 'enum can only have one key');
        Object.assign(this, args);
    }

    get enum() {
        return Object.keys(this)[0];
    }
}

class Account extends BaseMessage { }

class PublicKey extends BaseMessage {

    static fromString(str) {
        if (!str.startsWith('ed25519:')) {
            // TODO: Support other key formats
            throw new Error(`Unrecognized PublicKey string: ${JSON.stringify(str)}`);
        }

        return new PublicKey({ keyType: 0, data: bs58.decode(str.replace(/^ed25519:/, '')) });
    }

    toString() {
        if (this.keyType != 0) {
            throw new Error(`Unrecognized PublicKey keyType: ${this.keyType}`);
        }

        return `ed25519:${bs58.encode(this.data)}`;
    }
}

class AccessKey extends BaseMessage { }

class AccessKeyPermission extends Enum { }

class FunctionCallPermission extends BaseMessage { }

class FullAccessPermission extends BaseMessage { }

const BORSH_SCHEMA = new Map([
    // TODO: Refactor schema with network.js
    [Account, {
        kind: 'struct',
        fields: [
            ['amount', 'u128'],
            ['locked', 'u128'],
            ['code_hash', ['u8', 32]],
            ['storage_usage', 'u64'],
        ]
    }],
    [PublicKey, { kind: 'struct', fields: [
        ['keyType', 'u8'],
        ['data', [32]]
    ]}],
    [AccessKey, { kind: 'struct', fields: [
        ['nonce', 'u64'],
        ['permission', AccessKeyPermission],
    ]}],
    [AccessKeyPermission, {kind: 'enum', field: 'enum', values: [
        ['functionCall', FunctionCallPermission],
        ['fullAccess', FullAccessPermission],
    ]}],
    [FunctionCallPermission, {kind: 'struct', fields: [
        ['allowance', {kind: 'option', type: 'u128'}],
        ['receiverId', 'string'],
        ['methodNames', ['string']],
    ]}],
    [FullAccessPermission, {kind: 'struct', fields: []}],
]);

module.exports = {
    BaseMessage,
    Enum,
    Account,
    PublicKey,
    AccessKey,
    AccessKeyPermission,
    FunctionCallPermission,
    FullAccessPermission,
    BORSH_SCHEMA,
};