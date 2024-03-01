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
class Signature extends BaseMessage {}

class AccessKey extends BaseMessage { }
class AccessKeyPermission extends Enum { }
class FunctionCallPermission extends BaseMessage { }
class FullAccessPermission extends BaseMessage { }

class Transaction extends BaseMessage {}
class SignedTransaction extends BaseMessage {}

class Action extends Enum {}
class CreateAccount extends BaseMessage {}
class DeployContract extends BaseMessage {}
class FunctionCall extends BaseMessage {}
class Transfer extends BaseMessage {}
class Stake extends BaseMessage {}
class AddKey extends BaseMessage {}
class DeleteKey extends BaseMessage {}
class DeleteAccount extends BaseMessage {}

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
    [Signature, { kind: 'struct', fields: [
        ['keyType', 'u8'],
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
    [SignedTransaction, {kind: 'struct', fields: [
        ['transaction', Transaction],
        ['signature', Signature]
    ]}],
    [Transaction, { kind: 'struct', fields: [
        ['signerId', 'string'],
        ['publicKey', PublicKey],
        ['nonce', 'u64'],
        ['receiverId', 'string'],
        ['blockHash', [32]],
        ['actions', [Action]]
    ]}],
    [Action, { kind: 'enum', field: 'enum', values: [
        ['createAccount', CreateAccount],
        ['deployContract', DeployContract],
        ['functionCall', FunctionCall],
        ['transfer', Transfer],
        ['stake', Stake],
        ['addKey', AddKey],
        ['deleteKey', DeleteKey],
        ['deleteAccount', DeleteAccount],
    ]}],
    [CreateAccount, { kind: 'struct', fields: [] }],
    [DeployContract, { kind: 'struct', fields: [
        ['code', ['u8']]
    ]}],
    [FunctionCall, { kind: 'struct', fields: [
        ['methodName', 'string'],
        ['args', ['u8']],
        ['gas', 'u64'],
        ['deposit', 'u128']
    ]}],
    [Transfer, { kind: 'struct', fields: [
        ['deposit', 'u128']
    ]}],
    [Stake, { kind: 'struct', fields: [
        ['stake', 'u128'],
        ['publicKey', PublicKey]
    ]}],
    [AddKey, { kind: 'struct', fields: [
        ['publicKey', PublicKey],
        ['accessKey', AccessKey]
    ]}],
    [DeleteKey, { kind: 'struct', fields: [
        ['publicKey', PublicKey]
    ]}],
    [DeleteAccount, { kind: 'struct', fields: [
        ['beneficiaryId', 'string']
    ]}],

]);

module.exports = {
    BaseMessage,
    Enum,
    Account,
    PublicKey,
    Signature,
    AccessKey,
    AccessKeyPermission,
    FunctionCallPermission,
    FullAccessPermission,
    Transaction,
    SignedTransaction,

    BORSH_SCHEMA,
};