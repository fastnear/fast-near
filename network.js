const assert = require('assert');
const { serialize, deserialize } = require('borsh');

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

class Handshake extends BaseMessage {}
class PublicKey extends BaseMessage {}
class EdgeInfo extends BaseMessage {}
class EdgeInfoToSign extends BaseMessage {}
class Signature extends BaseMessage {}
class PeerChainInfoV2 extends BaseMessage {}
class GenesisId extends BaseMessage {}
class SocketAddrV4 extends BaseMessage {}
class SocketAddrV6 extends BaseMessage {}
class SocketAddr extends Enum {}
class PeerInfo extends BaseMessage {}
class HandshakeFailure extends BaseMessage {}
class HandshakeFailureReason extends Enum {}
class ProtocolVersionMismatch extends BaseMessage {}
class GenesisMismatch extends BaseMessage {}
class InvalidTarget extends BaseMessage {}
class LastEdge extends BaseMessage {}
class SyncData extends BaseMessage {}
class Edge extends BaseMessage {}
class RemovalInfo extends BaseMessage {}
class AnnounceAccount extends BaseMessage {}
class RequestUpdateNonce extends BaseMessage {}
class ResponseUpdateNonce extends BaseMessage {}
class PeersRequest extends BaseMessage {}
class PeersResponse extends BaseMessage {}
class BlockHeadersRequest extends BaseMessage {}
class BlockHeaders extends BaseMessage {}
class BlockRequest extends BaseMessage {}
class Block extends Enum {}
class BlockV1 extends BaseMessage {}
class BlockV2 extends BaseMessage {}
class BlockHeader extends Enum {}
class BlockHeaderV1 extends BaseMessage {}
class BlockHeaderV2 extends BaseMessage {}
class BlockHeaderV3 extends BaseMessage {}
class BlockHeaderInnerLite extends BaseMessage {}
class BlockHeaderInnerRest extends BaseMessage {}
class BlockHeaderInnerRestV2 extends BaseMessage {}
class BlockHeaderInnerRestV3 extends BaseMessage {}
class ShardChunkHeader extends Enum {}
class ShardChunkHeaderV1 extends BaseMessage {}
class ShardChunkHeaderV2 extends BaseMessage {}
class ShardChunkHeaderV3 extends BaseMessage {}
class ShardChunkHeaderInner extends Enum {}
class ShardChunkHeaderInnerV1 extends BaseMessage {}
class ShardChunkHeaderInnerV2 extends BaseMessage {}
class ValidatorStake extends Enum {}
class ValidatorStakeV1 extends BaseMessage {}
class ValidatorStakeV2 extends BaseMessage {}
class Transaction extends BaseMessage {}
class RoutedMessage extends BaseMessage {}
class Disconnect extends BaseMessage {}
class Challenge extends BaseMessage {}
class PeerMessage extends Enum {}

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
        ['last_edge', LastEdge],
        ['sync', SyncData],
        ['request_update_nonce', RequestUpdateNonce],
        ['response_update_nonce', ResponseUpdateNonce],
        ['peers_request', PeersRequest],
        ['peers_response', PeersResponse],
        ['block_headers_request', BlockHeadersRequest],
        ['block_headers', BlockHeaders],
        ['block_request', BlockRequest],
        ['block', Block],
        ['transaction', Transaction],
        ['routed', RoutedMessage],
        ['disconnect', Disconnect],
        ['challenge', Challenge],
    ]}],
    [HandshakeFailure, { kind: 'struct', fields: [
        ['peer_info', PeerInfo],
        ['failure_reason', HandshakeFailureReason]
    ]}],
    [HandshakeFailureReason, { kind: 'enum', field: 'enum', values: [
        ['protocol_version_mismatch', ProtocolVersionMismatch],
        ['genesis_mismatch', GenesisMismatch],
        ['invalid_target', InvalidTarget],
    ]}],
    [ProtocolVersionMismatch, { kind: 'struct', fields: [
        ['version', 'u32'],
        ['oldest_supported_version', 'u32'],
    ]}],
    [GenesisMismatch, { kind: 'struct', fields: [
        ['genesis_id', GenesisId],
    ]}],
    [InvalidTarget, { kind: 'struct', fields: [
    ]}],
    [SocketAddrV6, { kind: 'struct', fields: [
        ['ip', ['u8', 16]],
        ['port', 'u16'],
    ]}],
    [PeerInfo,  { kind: 'struct', fields: [
        ['id', PublicKey],
        ['addr', { kind: 'option', type: SocketAddr } ],
        ['account_id', { kind: 'option', type: 'string' } ],
    ]}],
    [SocketAddr, { kind: 'enum', values: [
        ['v4', SocketAddrV4],
        ['v6', SocketAddrV6],
    ]}],
    [SocketAddrV4,  { kind: 'struct', fields: [
        ['ip', ['u8', 4]],
        ['port', 'u16'],
    ]}],
    [SocketAddrV6, { kind: 'struct', fields: [
        ['ip', ['u8', 16]],
        ['port', 'u16'],
    ]}],
    [Block, { kind: 'enum', field: 'enum', values: [
        ['v1', BlockV1],
        ['v2', BlockV2],
    ]}],
    [BlockV1, { kind: 'struct', fields: [
        ['header', BlockHeader],
        ['chunks', [ShardChunkHeaderV1]],
        ['challenges', []], // TODO
        ['vrf_value', [32]],
        ['vrf_proof', [64]],
    ]}],
    [BlockV2, { kind: 'struct', fields: [
        ['header', BlockHeader],
        ['chunks', [ShardChunkHeader]],
        ['challenges', []], // TODO
        ['vrf_value', [32]],
        ['vrf_proof', [64]],
    ]}],
    [BlockHeader, { kind: 'enum', field: 'enum', values: [
        ['v1', BlockHeaderV1],
        ['v2', BlockHeaderV2],
        ['v3', BlockHeaderV3]
    ]}],
    [BlockHeaderV1, { kind: 'struct', fields: [
        ['prev_hash', [32]],
        ['inner_lite', BlockHeaderInnerLite],
        ['inner_rest', BlockHeaderInnerRest],
        ['signature', Signature],
    ]}],
    [BlockHeaderV2, { kind: 'struct', fields: [
        ['prev_hash', [32]],
        ['inner_lite', BlockHeaderInnerLite],
        ['inner_rest', BlockHeaderInnerRestV2],
        ['signature', Signature],
    ]}],
    [BlockHeaderV3, { kind: 'struct', fields: [
        ['prev_hash', [32]],
        ['inner_lite', BlockHeaderInnerLite],
        ['inner_rest', BlockHeaderInnerRestV3],
        ['signature', Signature],
    ]}],
    [BlockHeaderInnerLite, { kind: 'struct', fields: [
        ['height', 'u64'],
        ['epoch_id', [32]],
        ['next_epoch_id', [32]],
        ['prev_state_root', [32]],
        ['outcome_root', [32]],
        ['timestamp', 'u64'],
        ['next_bp_hash', [32]],
        ['block_merkle_root', [32]],
    ]}],
    [BlockHeaderInnerRest, { kind: 'struct', fields: [
        ['chunk_receipts_root', [32]],
        ['chunk_headers_root', [32]],
        ['chunk_tx_root', [32]],
        ['chunks_included', 'u64'],
        ['challenges_root', [32]],
        ['random_value', [32]],
        ['validator_proposals', [ValidatorStakeV1]],
        ['chunk_mask', ['u8']],
        ['gas_price', 'u128'],
        ['total_supply', 'u128'],
        ['challenges_result', []], // TODO
        ['last_final_block', [32]],
        ['last_ds_final_block', [32]],
        ['approvals', [{
            'kind': 'option',
            'type': Signature
        }]],
        ['latest_protocol_verstion', 'u32'],
    ]}],
    [BlockHeaderInnerRestV2, { kind: 'struct', fields: [
        ['chunk_receipts_root', [32]],
        ['chunk_headers_root', [32]],
        ['chunk_tx_root', [32]],
        ['challenges_root', [32]],
        ['random_value', [32]],
        ['validator_proposals', [ValidatorStakeV1]],
        ['chunk_mask', ['u8']],
        ['gas_price', 'u128'],
        ['total_supply', 'u128'],
        ['challenges_result', []], // TODO
        ['last_final_block', [32]],
        ['last_ds_final_block', [32]],
        ['approvals', [{
            'kind': 'option',
            'type': Signature
        }]],
        ['latest_protocol_verstion', 'u32'],
    ]}],
    [BlockHeaderInnerRestV3, { kind: 'struct', fields: [
        ['chunk_receipts_root', [32]],
        ['chunk_headers_root', [32]],
        ['chunk_tx_root', [32]],
        ['challenges_root', [32]],
        ['random_value', [32]],
        ['validator_proposals', [ValidatorStake]],
        ['chunk_mask', ['u8']],
        ['gas_price', 'u128'],
        ['total_supply', 'u128'],
        ['challenges_result', []], // TODO
        ['last_final_block', [32]],
        ['last_ds_final_block', [32]],
        ['block_ordinal', 'u64'],
        ['prev_height', 'u64'],
        ['epoch_sync_data_hash', {
            'kind': 'option',
            'type': [32]
        }],
        ['approvals', [{
            'kind': 'option',
            'type': Signature
        }]],
        ['latest_protocol_verstion', 'u32'],
    ]}],
    [ShardChunkHeader, { kind: 'enum', field: 'enum', values: [
        ['v1', ShardChunkHeaderV1],
        ['v2', ShardChunkHeaderV2],
        ['v3', ShardChunkHeaderV3]
    ]}],
    [ShardChunkHeaderV1, { kind: 'struct', fields: [
        ['inner', ShardChunkHeaderInnerV1],
        ['height_included', 'u64'],
        ['signature', Signature],
    ]}],
    [ShardChunkHeaderV2, { kind: 'struct', fields: [
        ['inner', ShardChunkHeaderInnerV1],
        ['height_included', 'u64'],
        ['signature', Signature],
    ]}],
    [ShardChunkHeaderV3, { kind: 'struct', fields: [
        ['inner', ShardChunkHeaderInner],
        ['height_included', 'u64'],
        ['signature', Signature],
    ]}],
    [ShardChunkHeaderInner, { kind: 'enum', field: 'enum', values: [
        ['v1', ShardChunkHeaderInnerV1],
        ['v2', ShardChunkHeaderInnerV2]
    ]}],
    [ShardChunkHeaderInnerV1, { kind: 'struct', fields: [
        ['prev_block_hash', [32]],
        ['prev_state_root', [32]],
        ['outcome_root', [32]],
        ['encoded_merkle_root', [32]],
        ['encoded_length', 'u64'],
        ['height_created', 'u64'],
        ['shard_id', 'u64'],
        ['gas_used', 'u64'],
        ['gas_limit', 'u64'],
        ['balance_burnt', 'u128'],
        ['outgoing_receipt_root', [32]],
        ['tx_root', [32]],
        ['validator_proposals', [ValidatorStakeV1]],
    ]}],
    [ShardChunkHeaderInnerV2, { kind: 'struct', fields: [
        ['prev_block_hash', [32]],
        ['prev_state_root', [32]],
        ['outcome_root', [32]],
        ['encoded_merkle_root', [32]],
        ['encoded_length', 'u64'],
        ['height_created', 'u64'],
        ['shard_id', 'u64'],
        ['gas_used', 'u64'],
        ['gas_limit', 'u64'],
        ['balance_burnt', 'u128'],
        ['outgoing_receipt_root', [32]],
        ['tx_root', [32]],
        ['validator_proposals', [ValidatorStake]],
    ]}],
    [ValidatorStake, { kind: 'enum', field: 'enum', values: [
        ['v1', ValidatorStakeV1],
        ['v2', ValidatorStakeV2]
    ]}],
    [ValidatorStakeV1, { kind: 'struct', fields: [
        ['account_id', 'string'],
        ['public_key', PublicKey],
        ['stake', 'u128'],
    ]}],
    [ValidatorStakeV2, { kind: 'struct', fields: [
        ['account_id', 'string'],
        ['public_key', PublicKey],
        ['stake', 'u128'],
        ['is_chunk_only', 'u8']
    ]}],
    [SyncData, { kind: 'struct', fields: [
        ['edges', [Edge]],
        ['accounts', [AnnounceAccount]],
    ]}],
    [Edge, { kind: 'struct', fields: [
        ['peer0', PublicKey],
        ['peer1', PublicKey],
        ['nonce', 'u64'],
        ['signature0', Signature],
        ['signature1', Signature],
        ['removal_info', { kind: 'option', type: RemovalInfo }],
    ]}],
    [RemovalInfo, { kind: 'struct', fields: [
        ['type', 'u8'], // TODO: check what is proper name
        ['signature', Signature]
    ]}],
    [AnnounceAccount, { kind: 'struct', fields: [
        ['account_id', 'string'],
        ['peer_id', PublicKey],
        ['epoch_id', [32]],
        ['signature', Signature],
    ]}],
    [BlockHeadersRequest, { kind: 'struct', fields: [
        ['hashes', [[32]]] // TODO: Check if name makes sense
    ]}],
    [PeersRequest, { kind: 'struct', fields: [] }],
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
const EventEmitter = require('events');

const sendMessage = (socket, message) => {
    const messageData = serialize(BORSH_SCHEMA, message);
    const length = Buffer.alloc(4);
    length.writeInt32LE(messageData.length, 0);

    socket.write(Buffer.concat([length, messageData]));
}

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

    sendMessage(socket, handshake);
});

let eventEmitter = new EventEmitter();
let unprocessedData = Buffer.alloc(0);
socket.on('data', (data) => {
    data = Buffer.concat([unprocessedData, data]);
    while (data.length > 4) {
        const length = data.readInt32LE(0);
        if (length > data.length - 4) {
            unprocessedData = data;
            return;
        }

        const message = deserialize(BORSH_SCHEMA, PeerMessage, data.slice(4, 4 + length));
        unprocessedData = data = data.slice(4 + length);

        eventEmitter.emit('message', message);
    };
});

socket.on('error', error => {
    console.log('error', error);
});

eventEmitter.on('message', message => {
    console.log('message', message.enum);
    if (message.handshake) {
        console.log('received handshake', message.handshake);

        sendMessage(socket, new PeerMessage({ peers_request: new PeersRequest()}));
    }
});
