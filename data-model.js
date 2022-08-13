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

class PublicKey extends Enum {

    static fromString(str) {
        if (str.startsWith('ed25519:')) {
            return new PublicKey({ ed25519: new PublicKeyED25519({ data: bs58.decode(str.replace(/^ed25519:/, '')) }) });
        }

        if (str.startsWith('secp256k1:')) {
            return new PublicKey({ secp256k1: new PublicKeySECP256K1({ data: bs58.decode(str.replace(/^secp256k1:/, '')) }) });
        }

        throw new Error(`Unrecognized PublicKey string: ${JSON.stringify(str)}`);
    }

    toString() {
        if (this.ed25519) {
            return `ed25519:${bs58.encode(this.ed25519.data)}`;
        }

        if (this.secp256k1) {
            return `secp256k1:${bs58.encode(this.secp256k1.data)}`;
        }

        throw new Error(`Unrecognized PublicKey: ${this.enum}`);
    }
}

class PublicKeyED25519 extends BaseMessage { }

class PublicKeySECP256K1 extends BaseMessage { }

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
    [PublicKey, { kind: 'enum', field: 'enum', values: [
        ['ed25519', PublicKeyED25519],
        ['secp256k1', PublicKeySECP256K1],
    ]}],
    [PublicKeyED25519, { kind: 'struct', fields: [
        ['data', [32]]
    ]}],
    [PublicKeySECP256K1, { kind: 'struct', fields: [
        ['data', [64]]
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