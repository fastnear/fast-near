const assert = require('assert');
const { serialize, deserialize } = require('borsh');

class Handshake {
    version;
    oldest_supported_version;
    peer_id;
    target_peer_id;
    listen_port;
    chain_info;
    edge_info;

    constructor(args) {
        Object.assign(this, args);
    }
}

class PublicKey {
    keyType;
    data;

    constructor(args) {
        Object.assign(this, args);
    }
}

class EdgeInfo {
    nonce;
    signature;

    constructor(args) {
        Object.assign(this, args);
    }
}

class Signature {
    keyType;
    data;

    constructor(args) {
        Object.assign(this, args);
    }
}

class PeerChainInfoV2 {
    genesis_id;
    height;
    tracked_shards;
    archival;

    constructor(args) {
        Object.assign(this, args);
    }
}

class GenesisId {
    chain_id;
    hash;

    constructor(args) {
        Object.assign(this, args);
    }
}

class SocketAddrV4 {
    ip;
    port;

    constructor(args) {
        Object.assign(this, args);
    }
}

class SocketAddrV6 {
    ip;
    port;

    constructor(args) {
        Object.assign(this, args);
    }
}

class SocketAddr {
    v4;
    v6;

    constructor(args) {
        assert(Object.keys(args).length == 1, 'enum can only have one key');
        Object.assign(this, args);
    }

    get enum() {
        return Object.keys(this)[0];
    }
}

class PeerInfo {
    id;
    addr;
    account_id;

    constructor(args) {
        Object.assign(this, args);
    }
}

class HandshakeFailure {
    peer_info;
    failure_reason;

    constructor(args) {
        Object.assign(this, args);
    }
}

class HandshakeFailureReason {
    protocol_version_mismatch;
    genesis_mismatch;
    invalid_target;

    constructor(args) {
        assert(Object.keys(args).length == 1, 'enum can only have one key');
        Object.assign(this, args);
    }

    get enum() {
        return Object.keys(this)[0];
    }
}

class ProtocolVersionMismatch {
    version;
    oldest_supported_version;

    constructor(args) {
        Object.assign(this, args);
    }
}

class GenesisMismatch {
    genesis_id;

    constructor(args) {
        Object.assign(this, args);
    }
}

class InvalidTarget {

}

class PeerMessage {
    handshake;
    handshake_failure;

    constructor(args) {
        assert(Object.keys(args).length == 1, 'enum can only have one key');
        Object.assign(this, args);
    }

    get enum() {
        return Object.keys(this)[0];
    }
}

const BORSH_SCHEMA = new Map([
    [Handshake, { kind: 'struct', fields: [
        ['version', 'u32'],
        ['oldest_supported_version', 'u32'],
        ['peer_id', PublicKey],
        ['target_peer_id', PublicKey],
        ['listen_port', { kind: 'option', type: 'u16' }],
        ['chain_info', 'u128'],
        ['edge_info', EdgeInfo],
    ]}],
    [PublicKey, { kind: 'struct', fields: [
        ['keyType', 'u8'],
        ['data', [32]]
    ]}],
    [Signature, { kind: 'struct', fields: [
        ['keyType', 'u8'],
        ['data', [64]]
    ]}],
    [EdgeInfo, { kind: 'struct', fields: [
        ['nonce', 'u64'],
        ['signature', Signature]
    ]}],
    [PeerChainInfoV2, { kind: 'struct', fields: [
        ['genesis_id', GenesisId],
        ['height', 'u64'],
        ['tracked_shards', ['u64']],
        ['archival', 'bool'],
    ]}],
    [GenesisId, { kind: 'struct', fields: [
        ['chain_id', 'string'],
        ['hash', [32]]
    ]}],
    [PeerMessage, { kind: 'enum', field: 'enum', values: [
        ['handshake', Handshake],
        ['handshake_failure', HandshakeFailure],
    ]}],
    [HandshakeFailure,  { kind: 'struct', fields: [
        ['peer_info', PeerInfo],
        ['failure_reason', HandshakeFailureReason]
    ]}],
    [HandshakeFailureReason, { kind: 'enum', field: 'enum', values: [
        ['protocol_version_mismatch', ProtocolVersionMismatch],
        ['genesis_mismatch', GenesisMismatch],
        ['invalid_target', InvalidTarget],
    ]}],
    [ProtocolVersionMismatch,  { kind: 'struct', fields: [
        ['version', 'u32'],
        ['oldest_supported_version', 'u32'],
    ]}],
    [GenesisMismatch,  { kind: 'struct', fields: [
        ['genesis_id', GenesisId],
    ]}],
    [InvalidTarget,  { kind: 'struct', fields: [
    ]}],
    [SocketAddrV6,  { kind: 'struct', fields: [
        ['ip', ['u8', 16]],
        ['port', 'u16'],
    ]}],
    [PeerInfo,  { kind: 'struct', fields: [
        ['id', PublicKey],
        ['addr', { kind: 'option', type: SocketAddr } ],
        ['account_id', { kind: 'option', type: 'string' } ],
    ]}],
    [SocketAddr,  { kind: 'enum', values: [
        ['v4', SocketAddrV4],
        ['v6', SocketAddrV6],
    ]}],
    [SocketAddrV4,  { kind: 'struct', fields: [
        ['ip', ['u8', 4]],
        ['port', 'u16'],
    ]}],
    [SocketAddrV6,  { kind: 'struct', fields: [
        ['ip', ['u8', 16]],
        ['port', 'u16'],
    ]}],
]);

const bs58 = require('bs58');
const net = require('net');

const socket = net.connect(24567, '34.94.158.10', () => {
    console.log('connected');

    // const pubkey = new PublicKey({ keyType: 0, data: new Uint8Array() })
    // console.log('pubkey', pubkey.keyType, pubkey.data)

    const publicKey = Buffer.alloc(32); // TODO: Real key
    const handshake = new PeerMessage({
        handshake: new Handshake({
            version: 0,
            oldest_supported_version: 0,
            peer_id: new PublicKey({ type: 0, data: publicKey }),
            target_peer_id:  new PublicKey({ type: 0, data: bs58.decode('4keFArc3M4SE1debUQWi3F1jiuFZSWThgVuA2Ja2p3Jv') }),
            listen_port: null,
            chain_info: '0',
            edge_info: new EdgeInfo({
                nonce: 0,
                signature: new Signature({ type: 0, data: Buffer.alloc(64) }),
            })
        })
    });

    const message = serialize(BORSH_SCHEMA, handshake);
    const length = Buffer.alloc(4);
    length.writeInt32LE(message.length, 0);

    socket.write(Buffer.concat([length, message]));
});

socket.on('data', (data) => {
    console.log('data', data.toString('hex'));

    const length = data.readInt32LE(0);
    assert(length == data.length - 4);

    const message = deserialize(BORSH_SCHEMA, PeerMessage, data.slice(4));
    console.log('message', message?.handshake_failure?.failure_reason || message);
});

socket.on('error', error => {
    console.log('error', error);
});