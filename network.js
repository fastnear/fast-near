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

class EdgeInfoToSign {
    peer0;
    peer1;
    nonce;

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
        ['chain_info', PeerChainInfoV2],
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
    [EdgeInfoToSign, { kind: 'struct', fields: [
        ['peer0', PublicKey],
        ['peer1', PublicKey],
        ['nonce', 'u64'],
    ]}],
    [PeerChainInfoV2, { kind: 'struct', fields: [
        ['genesis_id', GenesisId],
        ['height', 'u64'],
        ['tracked_shards', ['u64']],
        ['archival', 'u8'],
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

const ed = require('@noble/ed25519');
const { sha256 } = require('@noble/hashes/lib/sha256');

const privateKey = ed.utils.randomPrivateKey(); // 32-byte Uint8Array or string.

const signEdgeInfo = async (nonce, peer0, peer1) => {
    if (Buffer.compare(peer0.data, peer1.data) > 0) {
        [peer1, peer0] = [peer0, peer1];
    }
    const data = serialize(BORSH_SCHEMA, new EdgeInfoToSign({ peer0, peer1, nonce }));
    const signature = await ed.sign(sha256(data), privateKey);
    return new Signature({ keyType: 0, data: Buffer.from(signature) });
}

const bs58 = require('bs58');
const net = require('net');

const socket = net.connect(24567, '127.0.0.1', async () => {
    console.log('connected');

    const publicKey = Buffer.from(await ed.getPublicKey(privateKey));
    const nonce = 1;
    const peer_id = new PublicKey({ keyType: 0, data: publicKey });
    const target_peer_id = new PublicKey({ keyType: 0, data: bs58.decode('2gYpfHjqJa5Ji3btBnScQrxgwx2Ya5NXnJoTDqJWY36c') });

    const handshake = new PeerMessage({
        handshake: new Handshake({
            version: 48,
            oldest_supported_version: 34,
            peer_id,
            target_peer_id,
            listen_port: 24567,
            chain_info: new PeerChainInfoV2({
                genesis_id: new GenesisId({
                    chain_id: 'localnet',
                    hash: Buffer.from('b2adf5f9273460d714aa622a92cd9445bcfa110cf6315bc20241552b4c61f0a1', 'hex')
                }),
                height: 71969465,
                tracked_shards: [0],
                archival: 0
            }),
            edge_info: new EdgeInfo({
                nonce,
                signature: await signEdgeInfo(nonce, peer_id, target_peer_id),
            })
        })
    });
    console.log('handshake', handshake);

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

    //console.log('hash', Buffer.from(message?.handshake_failure?.failure_reason?.genesis_mismatch?.genesis_id?.hash).toString('hex'))
});

socket.on('error', error => {
    console.log('error', error);
});
