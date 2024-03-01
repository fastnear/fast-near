const assert = require('assert');
const { serialize, deserialize } = require('borsh');
const { BaseMessage, Enum, PublicKey, SignedTransaction, Signature } = require('./data-model');

class Handshake extends BaseMessage {}
class EdgeInfo extends BaseMessage {}
class EdgeInfoToSign extends BaseMessage {}
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
class RoutedMessage extends BaseMessage {}
class Disconnect extends BaseMessage {}
class Challenge extends BaseMessage {}
class PeerMessage extends Enum {}
class PeerIdOrHash extends Enum {}
class RoutedMessageBody extends Enum {}
class RoutedMessageToSign extends BaseMessage {}
class Approval extends BaseMessage {}
class StateResponseInfoV1 extends BaseMessage {}
class PartialEncodedChunkRequestMsg extends BaseMessage {}
class PartialEncodedChunkResponseMsg extends BaseMessage {}
class PartialEncodedChunkV1 extends BaseMessage {}
class PartialEncodedChunkV2 extends BaseMessage {}
class PartialEncodedChunkPart extends BaseMessage {}
class PingPong extends BaseMessage {}
class PartialEncodedChunk extends Enum {}
class StateResponseInfo extends BaseMessage {}
class PartialEncodedChunkForwardMsg extends BaseMessage {}
class StateRequestHeader extends BaseMessage {}
class ShardStateSyncResponseV1 extends BaseMessage {}
class ShardStateSyncResponseHeaderV1 extends BaseMessage {}
class SyncPart extends BaseMessage {}
class MerklePath extends BaseMessage {}
class MerklePathItem extends BaseMessage {}
class Direction extends Enum {}
class UnitType extends BaseMessage {}
class ShardChunk extends BaseMessage {}
class Receipt extends BaseMessage {}
class ReceiptEnum extends Enum {}
class ActionReceipt extends BaseMessage {}
class AccessKey extends BaseMessage {}
class AccessKeyPermission extends BaseMessage {}
class FunctionCallPermission extends BaseMessage {}
class FullAccessPermission extends BaseMessage {}
class DataReceiver extends BaseMessage {}
class DataReceipt extends BaseMessage {}
class ReceiptProof extends BaseMessage {}
class ReceiptProofResponse extends BaseMessage {}
class ShardProof extends BaseMessage {}
class StateRootNode extends BaseMessage {}
class RootProof extends BaseMessage {}
class TransactionReceipt extends BaseMessage {}

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
        ['transaction', SignedTransaction],
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
    [PeersResponse, { kind: 'struct', fields: [
        ['peers', [PeerInfo]],
    ]}],
    [BlockRequest, { kind: 'struct', fields: [
        ['block_hash', [32]],
    ]}],
    [RoutedMessage, { kind: 'struct', fields: [
        ['target', PeerIdOrHash],
        ['author', PublicKey],
        ['signature', Signature],
        ['ttl', 'u8'],
        ['body', RoutedMessageBody],
    ]}],
    [RoutedMessageToSign, { kind: 'struct', fields: [
        ['target', PeerIdOrHash],
        ['author', PublicKey],
        ['body', RoutedMessageBody],
    ]}],
    [PeerIdOrHash, { kind: 'enum', field: 'enum', values: [
        ['peer_id', PublicKey],
        ['hash', [32]],
    ]}],
    [RoutedMessageBody, { kind: 'enum', field: 'enum', values: [
        ['block_approval', Approval],
        ['forward_tx', SignedTransaction],
        ['tx_status_request', ['string', [32]]],
        ['tx_status_response', false],  // TODO
        ['query_request', null],  // TODO
        ['query_response', false],  // TODO
        ['receipt_outcome_request', [32]],
        ['receipt_outcome_response', false],  // TODO
        ['state_request_header', StateRequestHeader],
        ['state_request_part', ['u64', [32], 'u64']],
        ['state_response_info', StateResponseInfoV1],
        ['partial_encoded_chunk_request', PartialEncodedChunkRequestMsg],
        ['partial_encoded_chunk_response', PartialEncodedChunkResponseMsg],
        ['partial_encoded_chunk', PartialEncodedChunkV1],
        ['ping', PingPong],
        ['pong', PingPong],
        ['versioned_partial_encoded_chunk', PartialEncodedChunk],
        ['versioned_state_response', StateResponseInfo],
        ['partial_encoded_chunk_forward', PartialEncodedChunkForwardMsg]
    ]}],
    [PingPong, { kind: 'struct', fields: [
        ['nonce', 'u64'],
        ['source', PublicKey]
    ]}],
    [StateRequestHeader, { kind: 'struct', fields: [
        ['shard_id', 'u64'],
        ['hash', [32]],
    ]}],
    [StateResponseInfoV1, { kind: 'struct', fields: [
        ['shard_id', 'u64'],
        ['sync_hash', [32]],
        ['state_response', ShardStateSyncResponseV1]
    ]}],
    [SyncPart, { kind: 'struct', fields: [
        ['part_id', 'u64'],
        ['data', ['u8']],
    ]}],
    [ShardStateSyncResponseV1, { kind: 'struct', fields: [
        ['header', { kind: 'option', type: ShardStateSyncResponseHeaderV1 }],
        ['part', { kind: 'option', type: SyncPart }],
    ]}],
    [Receipt, { kind: 'struct', fields: [
        ['predecessor_id', 'string'],
        ['receiver_id', 'string'],
        ['receipt_id', [32]],
        ['receipt', ReceiptEnum],
    ]}],
    [ReceiptEnum, { kind: 'enum', field: 'enum', values: [
        ['Action', ActionReceipt],
        ['Data', DataReceipt],
    ]}],
    [ActionReceipt, { kind: 'struct', fields: [
        ['signer_id', 'string'],
        ['signer_public_key', PublicKey],
        ['gas_price', 'u128'],
        ['output_data_receivers', [DataReceiver]],
        ['input_data_ids', [[32]]],
        ['actions', [Action]],
    ]}],
    [DataReceiver, { kind: 'struct', fields: [
        ['data_id', [32]],
        ['receiver_id', 'string'],
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
    [DataReceipt, { kind: 'struct', fields: [
        ['data_id', [32]],
        ['data', { kind: 'option', type: ['u8'] }],
    ]}],
    [ReceiptProof, { kind: 'struct', fields: [
        ['receipts', [Receipt]],
        ['proof', ShardProof]
    ]}],
    [ReceiptProofResponse, { kind: 'struct', fields: [
        ['hash', [32]],
        ['proofs', [ReceiptProof]]
    ]}],
    [RootProof, { kind: 'struct', fields: [
        ['hash', [32]],
        ['path', MerklePath]
    ]}],
    [ShardStateSyncResponseHeaderV1, { kind: 'struct', fields: [
        ['chunk', ShardChunk], ['chunk_proof', MerklePath],
        ['prev_chunk_header', { kind: 'option', type: ShardChunkHeaderV1}],
        ['prev_chunk_proof', { kind: 'option', type: MerklePath }],
        ['incoming_receipts_proofs', [ReceiptProofResponse]], // TODO
        ['root_proofs', [[RootProof]]], // TODO
        ['state_root_node', StateRootNode]
    ]}],
    [PartialEncodedChunk, { kind: 'enum', field: 'enum', values: [
        ['V1', PartialEncodedChunkV1],
        ['V2', PartialEncodedChunkV2]
    ]}],
    [PartialEncodedChunkV1,  { kind: 'struct', fields: [
        ['header', ShardChunkHeaderV1],
        ['parts', [PartialEncodedChunkPart]],
        ['receipts', [ReceiptProof]]
    ]}],
    [PartialEncodedChunkV2, { kind: 'struct', fields: [
        ['header', ShardChunkHeader],
        ['parts', [PartialEncodedChunkPart]],
        ['receipts', [ReceiptProof]]
    ]}],
    [PartialEncodedChunkRequestMsg, { kind: 'struct', fields: [
        ['chunk_hash', [32]],
        ['part_ords', ['u64']],
        ['tracking_shards', ['u64']]
    ]}],
    [PartialEncodedChunkResponseMsg, { kind: 'struct', fields: [
        ['chunk_hash', [32]],
        ['parts', [PartialEncodedChunkPart]],
        ['receipts', [ReceiptProof]]
    ]}],
    [PartialEncodedChunkPart, { kind: 'struct', fields: [
        ['part_ord', 'u64'],
        ['part', ['u8']],
        ['merkle_proof', MerklePath],
    ]}],
    [MerklePath, { kind: 'struct', fields: [
        ['items', [MerklePathItem]],
    ]}],
    [MerklePathItem, { kind: 'struct', fields: [
        ['hash', [32]],
        ['direction', Direction],
    ]}],
    [Direction, {kind: 'enum', field: 'enum', values: [
        ['Left', UnitType],
        ['Right', UnitType],
    ]}],
    [UnitType, { kind: 'struct', fields: []}],
    [ShardProof, { kind: 'struct', fields: [
        ['from_shard_id', 'u64'],
        ['to_shard_id', 'u64'],
        ['proof', MerklePath]
    ]}],
    [TransactionReceipt, { kind: 'struct', fields: [
        ['transactions', [SignedTransaction]],
        ['receipts', [Receipt]]
    ]}],
]);

const ed = require('@noble/ed25519');
const sha256  = require('./utils/sha256');

const privateKey = ed.utils.randomPrivateKey(); // 32-byte Uint8Array or string.

const signObject = async (obj) => {
    const data = serialize(BORSH_SCHEMA, obj);
    const signature = await ed.sign(sha256(data), privateKey);
    return new Signature({ keyType: 0, data: Buffer.from(signature) });
}

const signEdgeInfo = async (nonce, peer0, peer1) => {
    if (Buffer.compare(peer0.data, peer1.data) > 0) {
        [peer1, peer0] = [peer0, peer1];
    }
    return signObject(new EdgeInfoToSign({ peer0, peer1, nonce }));
}

const bs58 = require('bs58');
const net = require('net');
const EventEmitter = require('events');
const { Buffer } = require('buffer');
const { BaseMessage } = require('./data-model');

const sendMessage = (socket, message) => {
    const messageData = serialize(BORSH_SCHEMA, message);
    const length = Buffer.alloc(4);
    length.writeInt32LE(messageData.length, 0);

    socket.write(Buffer.concat([length, messageData]));
}

const target_peer_id = new PublicKey({ keyType: 0, data: bs58.decode('2gYpfHjqJa5Ji3btBnScQrxgwx2Ya5NXnJoTDqJWY36c') });
let peer_id;

const NODE_ADDRESS = process.env.NODE_ADDRESS || '127.0.0.1'
const NUM_TOTAL_PARTS = 50;
const NUM_DATA_PARTS = Math.floor((NUM_TOTAL_PARTS - 1) / 3);

const socket = net.connect(24567, NODE_ADDRESS, async () => {
    console.log('connected');

    const publicKey = Buffer.from(await ed.getPublicKey(privateKey));
    const nonce = 1;
    peer_id = new PublicKey({ keyType: 0, data: publicKey });
    console.log('peer_id', bs58.encode(peer_id.data))

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
                height: 3138788, // TODO: Update to avoid HeightFraud?
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

const sendRoutedMessage = async (messageBodyObj) => {
    // console.log('sendRoutedMessage', messageBodyObj);
    const messageToSign = new RoutedMessageToSign({
        target: new PeerIdOrHash({ peer_id: target_peer_id }),
        author: peer_id,
        body: new RoutedMessageBody(messageBodyObj)
    });
    sendMessage(socket, new PeerMessage({
        routed: new RoutedMessage({
            target: messageToSign.target,
            author: messageToSign.author,
            signature: await signObject(messageToSign),
            ttl: 100,
            body: messageToSign.body
        })
    }));
}

const chunkHash = (chunkHeader) => {
    // TODO: v1 and v3?
    if (chunkHeader.v2) {
        const { inner } = chunkHeader.v2;
        const serialized = serialize(BORSH_SCHEMA, inner);
        const innerHash = sha256(serialized);
        return sha256(Buffer.concat([innerHash, inner.encoded_merkle_root]));
    }
}

const chunkHeaders = {};

eventEmitter.on('message', async message => {
    console.log('message', message.enum);
    if (message.handshake) {
        console.log('received handshake');

        // sendMessage(socket, new PeerMessage({ peers_request: new PeersRequest() }));
        // sendMessage(socket, new PeerMessage({
        //     // NOTE: block is returned by previous block hash?
        //     block_request: new BlockRequest({ block_hash: bs58.decode('5xFjQhte3amNExUZxtLRaKEzQ7qofStXm5WjU63sjhM4') })
        // }));

        sendRoutedMessage({
            ping: new PingPong({ nonce: 0, source: peer_id })
        });

        sendRoutedMessage({
            state_request_header: new StateRequestHeader({
                shard_id: 0,
                // NOTE: Should this be prev hash of epoch start block hash?
                hash: bs58.decode('2i88qoGUT1HJ7rQoc5QrR492K1tpAKbEyyRF7h3YS6zS')
            })
        });
    }

    if (message.block) {
        const header = message.block.v2.header.v2;
        console.log('block', bs58.encode(header.prev_hash), header.inner_lite.height.toString());
        // console.log(bs58.encode(header.inner_lite.prev_state_root));
        // console.log('header', header);
        // console.log('chunks', message.block.v2.chunks.map(it => it.v2));

        // const { prev_state_root } = message.block.v2.chunks[0].v2.inner;
        // console.log('prev_state_root', bs58.encode(prev_state_root));

        // sendRoutedMessage({
        //     state_request_header: new StateRequestHeader({
        //         shard_id: 0,
        //         hash: prev_state_root
        //     })
        // });

        // sendRoutedMessage({
        //     block_request: new BlockRequest({
        //         block_hash: header.prev_hash
        //     })
        // });

        const { chunks } = message.block.v2;

        for (let chunk of chunks) {
            const hash = chunkHash(chunk);
            chunkHeaders[bs58.encode(hash)] = chunk;
            sendRoutedMessage({
                partial_encoded_chunk_request: new PartialEncodedChunkRequestMsg({
                    chunk_hash: hash,
                    part_ords: [...Array(NUM_DATA_PARTS)].map((_, i) => i),
                    tracking_shards: [0]
                })
            });
        }
    }

    if (message.routed) {
        console.log('routed', message.routed.body.enum);

        const {
            body: {
                state_response_info,
                partial_encoded_chunk_response,
            }
        } = message.routed;

        if (state_response_info) {
            console.log('state_response_info', state_response_info);
        }

        if (partial_encoded_chunk_response) {
            // console.log('partial_encoded_chunk_response', partial_encoded_chunk_response);
            // console.log('parts', partial_encoded_chunk_response.parts.map(({ part }) => Buffer.from(part).toString('hex')));
            const { chunk_hash } = partial_encoded_chunk_response;
            const chunk = chunkHeaders[bs58.encode(chunk_hash)];
            if (!chunk) {
                console.error('cannot find chunk header:', bs58.encode(chunk_hash))
                return;
            }

            const { encoded_length } = chunk.v2.inner;
            const allParts = Buffer.concat(partial_encoded_chunk_response.parts.map(({ part }) => Buffer.from(part)));
            const { transactions, receipts } = deserialize(BORSH_SCHEMA, TransactionReceipt, allParts.slice(0, encoded_length));
            console.log('transactions', transactions);
            console.log('receipts', receipts);
        }
    }
});
