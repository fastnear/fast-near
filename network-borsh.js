const { serialize, deserialize } = require('borsh');
const { BaseMessage, Enum, PublicKey } = require('./data-model');

class Handshake extends BaseMessage {}
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
class PeerIdOrHash extends Enum {}
class RoutedMessageBody extends Enum {}
class RoutedMessageToSign extends BaseMessage {}
class Approval extends BaseMessage {}
class SignedTransaction extends BaseMessage {}
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
class Action extends Enum {}
class CreateAccount extends BaseMessage {}
class DeployContract extends BaseMessage {}
class FunctionCall extends BaseMessage {}
class Transfer extends BaseMessage {}
class Stake extends BaseMessage {}
class AddKey extends BaseMessage {}
class DeleteKey extends BaseMessage {}
class DeleteAccount extends BaseMessage {}
class DataReceiver extends BaseMessage {}
class DataReceipt extends BaseMessage {}
class ReceiptProof extends BaseMessage {}
class ReceiptProofResponse extends BaseMessage {}
class ShardProof extends BaseMessage {}
class StateRootNode extends BaseMessage {}
class RootProof extends BaseMessage {}
class TransactionReceipt extends BaseMessage {}

const { BORSH_SCHEMA: BASE_SCHEMA } = require('./data-model');

// Merge 2 Maps in one function call

const BORSH_SCHEMA = new Map([...BASE_SCHEMA.entries(),
    [Handshake, { kind: 'struct', fields: [
        ['version', 'u32'],
        ['oldest_supported_version', 'u32'],
        ['peer_id', PublicKey],
        ['target_peer_id', PublicKey],
        ['listen_port', { kind: 'option', type: 'u16' }],
        ['chain_info', PeerChainInfoV2],
        ['edge_info', EdgeInfo],
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

module.exports = {
    BORSH_SCHEMA,
    PeerChainInfoV2,
    GenesisId,
    Edge,
    EdgeInfo,
    EdgeInfoToSign,
    Signature,
    PingPong,
    RoutedMessageToSign,
    PeerIdOrHash,
    RoutedMessageBody,
    RoutedMessage,
    StateRequestHeader,
    AnnounceAccount,
    Block,
    PartialEncodedChunkRequestMsg,
};