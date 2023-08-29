const { readProto, writeProtoField } = require('./utils/proto');

// PeerMessage is a wrapper of all message types exchanged between NEAR nodes.
// The wire format of a single message M consists of len(M)+4 bytes:
// <len(M)> : 4 bytes : little endian uint32
// <M> : N bytes : binary encoded protobuf PeerMessage M
// message PeerMessage {
//     // Leaving 1,2,3 unused allows us to ensure that there will be no collision
//     // between borsh and protobuf encodings:
//     // https://docs.google.com/document/d/1gCWmt9O-h_-5JDXIqbKxAaSS3Q9pryB1f9DDY1mMav4/edit
//     reserved 1,2,3;
//     // Deprecated fields.
//     reserved 20,21,22,23,24;

//     // Inter-process tracing information.
//     TraceContext trace_context = 26;

//     oneof message_type {
//       // Handshakes for TIER1 and TIER2 networks are considered separate,
//       // so that a node binary which doesn't support TIER1 connection won't
//       // be even able to PARSE the handshake. This way we avoid accidental
//       // connections, such that one end thinks it is a TIER2 connection and the
//       // other thinks it is a TIER1 connection. As currently both TIER1 and TIER2
//       // connections are handled by the same PeerActor, both fields use the same
//       // underlying message type. If we ever decide to separate the handshake
//       // implementations, we can copy the Handshake message type defition and
//       // make it evolve differently for TIER1 and TIER2.
//       Handshake tier1_handshake = 27;
//       Handshake tier2_handshake = 4;

//       HandshakeFailure handshake_failure = 5;
//       LastEdge last_edge = 6;
//       RoutingTableUpdate sync_routing_table = 7;

//       UpdateNonceRequest update_nonce_request = 8;
//       UpdateNonceResponse update_nonce_response = 9;

//       SyncAccountsData sync_accounts_data = 25;

//       PeersRequest peers_request = 10;
//       PeersResponse peers_response = 11;

//       BlockHeadersRequest block_headers_request = 12;
//       BlockHeadersResponse block_headers_response = 13;

//       BlockRequest block_request = 14;
//       BlockResponse block_response = 15;

//       SignedTransaction transaction = 16;
//       RoutedMessage routed = 17;
//       Disconnect disconnect = 18;
//       Challenge challenge = 19;
//     }
//   }
function readPeerMessage(data) {
    let peerMessage = readProto(data, (fieldNumber, value, result) => {
        switch (fieldNumber) {
            case 4:
                // tier2_handshake
                result.handshake = readHandshake(value);
                break;
            case 5:
                result.handshake_failure = readHandshakeFailure(value);
                break;
            case 6:
                result.last_edge = readLastEdge(value);
                break;
            case 7:
                result.sync_routing_table = readRoutingTableUpdate(value);
                break;
            case 8:
                result.update_nonce_request = readUpdateNonceRequest(value);
                break;
            case 9:
                result.update_nonce_response = readUpdateNonceResponse(value);
                break;
            case 10:
                result.peers_request = readPeersRequest(value);
                break;
            case 11:
                result.peers_response = readPeersResponse(value);
                break;
            case 12:
                result.block_headers_request = readBlockHeadersRequest(value);
                break;
            case 13:
                result.block_headers_response = readBlockHeadersResponse(value);
                break;
            case 14:
                result.block_request = readBlockRequest(value);
                break;
            case 15:
                result.block_response = readBlockResponse(value);
                break;
            case 16:
                result.transaction = readSignedTransaction(value);
                break;
            case 17:
                result.routed = readRoutedMessage(value);
                break;
            case 18:
                result.disconnect = readDisconnect(value);
                break;
            case 19:
                result.challenge = readChallenge(value);
                break;
            case 25:
                result.sync_accounts_data = readSyncAccountsData(value);
                break;
            case 26:
                result.trace_context = readTraceContext(value);
                break;
            case 27:
                // tier1_handshake
                result.handshake = readHandshake(value);
                break;
            default:
                throw new Error(`Unsupported PeerMessage field number: ${fieldNumber}`);
        }
    });
    return peerMessage;
}

function writePeerMessage(peerMessage) {
    const fields = [];
    if (peerMessage.handshake) {
        fields.push(writeProtoField(4, 2, writeHandshake(peerMessage.handshake)));
    }
    if (peerMessage.handshake_failure) {
        fields.push(writeProtoField(5, 2, writeHandshakeFailure(peerMessage.handshake_failure)));
    }

    // TODO: all other fields

    if (peerMessage.routed) {
        fields.push(writeProtoField(17, 2, writeRoutedMessage(peerMessage.routed)));
    }
    console.log('fields', fields);
    // TODO
    return Buffer.concat(fields);
}

// Handshake is the first message exchanged after establishing a TCP connection.
// If A opened a connection B, then
// 1. A sends Handshake to B.
// 2a. If B accepts the handshake, it sends Handshake to A and connection is established.
// 2b. If B rejects the handshake, it sends HandshakeFailure to A.
//     A may retry the Handshake with a different payload.
// message Handshake {
//     // The protocol_version that the sender wants to use for communication.
//     // Currently NEAR protocol and NEAR network protocol are versioned together
//     // (it may change in the future), however peers may communicate with the newer version
//     // of the NEAR network protol, than the NEAR protocol version approved by the quorum of
//     // the validators. If B doesn't support protocol_version, it sends back HandshakeFailure
//     // with reason ProtocolVersionMismatch.
//     uint32 protocol_version = 1;
//     // Oldest version of the NEAR network protocol that the peer supports.
//     uint32 oldest_supported_version = 2;
//     // PeerId of the sender.
//     PublicKey sender_peer_id = 3;
//     // PeerId of the receiver that the sender expects.
//     // In case of mismatch, receiver sends back HandshakeFailure with
//     // reason InvalidTarget.
//     PublicKey target_peer_id = 4;
//     // TCP port on which sender is listening for inbound connections.
//     uint32 sender_listen_port = 5;
//     // Basic info about the NEAR chain that the sender belongs to.
//     // Sender expects receiver to belong to the same chain.
//     // In case of mismatch, receiver sends back HandshakeFailure with
//     // reason GenesisMismatch.
//     PeerChainInfo sender_chain_info = 6;
//     // Edge (sender,receiver) signed by sender, which once signed by
//     // receiver may be broadcasted to the network to prove that the
//     // connection has been established.
//     // In case receiver accepts the Handshake, it sends back back a Handshake
//     // containing his signature in this field.
//     // WARNING: this field contains a signature of (sender_peer_id,target_peer_id,nonce) tuple,
//     // which currently the only thing that we have as a substitute for a real authentication.
//     // TODO(gprusak): for TIER1 authentication is way more important than for TIER2, so this
//     // thing should be replaced with sth better.
//     PartialEdgeInfo partial_edge_info = 7;
//     // See description of OwnedAccount.
//     AccountKeySignedPayload owned_account = 8; // optional
//     reserved 9; // https://github.com/near/nearcore/pull/9191
//   }

function readHandshake(data) {
    return readProto(data, (fieldNumber, value, handshake) => {
        switch (fieldNumber) {
            case 1:
                handshake.protocol_version = value;
                break;
            case 2:
                handshake.oldest_supported_version = value;
                break;
            case 3:
                handshake.sender_peer_id = readPublicKey(value);
                break;
            case 4:
                handshake.target_peer_id = readPublicKey(value);
                break;
            case 5:
                handshake.sender_listen_port = value;
                break;
            case 6:
                // handshake.sender_chain_info = readpeerchaininfo(value);
                break;
            case 7:
                // handshake.partial_edge_info = readPartialEdgeInfo(value);
                break;
            case 8:
                // handshake.owned_account = readAccountKeySignedPayload(value);
                break;
            default:
                throw new Error(`Unsupported Handshake field number: ${fieldNumber}`);
        }
    });
}

function writeHandshake(handshake) {
    console.log('writeHandshake', handshake);
    const fields = [];
    if (handshake.protocol_version) {
        fields.push(writeProtoField(1, 0, handshake.protocol_version));
    }
    if (handshake.oldest_supported_version) {
        fields.push(writeProtoField(2, 0, handshake.oldest_supported_version));
    }
    if (handshake.sender_peer_id) {
        fields.push(writeProtoField(3, 2, writePublicKey(handshake.sender_peer_id)));
    }
    if (handshake.target_peer_id) {
        // NOTE: this is PublicKey borsh wrapper
        fields.push(writeProtoField(4, 2, writePublicKey(handshake.target_peer_id)));
    }
    if (handshake.sender_listen_port) {
        fields.push(writeProtoField(5, 0, handshake.sender_listen_port));
    }
    if (handshake.sender_chain_info) {
        fields.push(writeProtoField(6, 2, writePeerChainInfo(handshake.sender_chain_info)));
    }
    if (handshake.partial_edge_info) {
        // NOTE: this is PartialEdgeInfo borsh wrapper
        fields.push(writeProtoField(7, 2, writePartialEdgeInfo(handshake.partial_edge_info)));
    }
    if (handshake.owned_account) {
        fields.push(writeProtoField(8, 2, writeAccountKeySignedPayload(handshake.owned_account)));
    }

    console.log('fields', fields);
    return Buffer.concat(fields);
}

// Wrapper of borsh-encoded RoutedMessage
// https://github.com/near/nearcore/blob/1a4edefd0116f7d1e222bc96569367a02fe64199/chain/network-primitives/src/network_protocol/mod.rs#L295
// message RoutedMessage {
//     bytes borsh = 1;
//     // Timestamp of creating the Routed message by its original author.
//     google.protobuf.Timestamp created_at = 2;
//     // Number of peers this routed message travelled through. Doesn't include the peer that created the message.
//     optional int32 num_hops = 3;
// }

function readRoutedMessage(data) {
    return readProto(data, (fieldNumber, value, routedMessage) => {
        switch (fieldNumber) {
            case 1:
                routedMessage.borsh = value;
                break;
            case 2:
                routedMessage.created_at = readTimestamp(value);
                break;
            case 3:
                routedMessage.num_hops = value;
                break;
            default:
                throw new Error(`Unsupported RoutedMessage field number: ${fieldNumber}`);
        }
    });
}

function writeRoutedMessage(routedMessage) {
    const fields = [];
    if (routedMessage.borsh !== undefined) {
        fields.push(writeProtoField(1, 2, routedMessage.borsh));
    }
    if (routedMessage.created_at !== undefined) {
        fields.push(writeProtoField(2, 2, writeTimestamp(routedMessage.created_at)));
    }
    if (routedMessage.num_hops !== undefined) {
        fields.push(writeProtoField(3, 0, routedMessage.num_hops));
    }

    return Buffer.concat(fields);
}

// NEAR chain Block.
// It might be send both as a response to BlockRequest,
// or unsolicitated in case a new Block is being broadcasted.
// message BlockResponse {
//     Block block = 1;
//   }
function readBlockResponse(data) {
    return readProto(data, (fieldNumber, value, blockResponse) => {
        switch (fieldNumber) {
            case 1:
                blockResponse.block = readBlock(value);
                break;
            default:
                throw new Error(`Unsupported BlockResponse field number: ${fieldNumber}`);
        }
    });
}

// Request to send a list of known healthy peers
// (i.e. considered honest and available by the receiver).
// max_peers limits the number of peers to send back.
// max_direct_peers limits the number of direct peers to send back.
// See PeersResponse below for the response.
// message PeersRequest {
//     optional uint32 max_peers = 1;
//     optional uint32 max_direct_peers = 2;
//   }
function readPeersRequest(data) {
    return readProto(data, (fieldNumber, value, peersRequest) => {
        switch (fieldNumber) {
            case 1:
                peersRequest.max_peers = value;
                break;
            case 2:
                peersRequest.max_direct_peers = value;
                break;
            default:
                throw new Error(`Unsupported PeersRequest field number: ${fieldNumber}`);
        }
    });
}

// SyncAccountData message can represent:
// - incremental sync (incremental = true, requesting_full_sync = false)
// - full sync request (incremental = false, requesting_full_sync = true)
// - full sync response (incremental = false, requesting_full_sync = false)
// message SyncAccountsData {
//     // Data about the (important) accounts,
//     // which should be broadcasted to the whole network.
//     // Contains AccountKeyPayload.account_data.
//     repeated AccountKeySignedPayload accounts_data = 1;
//     // Indicates whether this message is an incremental sync (true), or
//     // a full sync (false). Useful for tracking time since the last full sync.
//     bool incremental = 2;
//     // Indicates that sender requests a full sync message in return.
//     // Useful for soliciting a full sync periodically.
//     bool requesting_full_sync = 3;
//   }
function readSyncAccountsData(data) {
    return readProto(data, (fieldNumber, value, syncAccountsData) => {
        switch (fieldNumber) {
            case 1:
                if (!syncAccountsData.accounts_data) {
                    syncAccountsData.accounts_data = [];
                }
                syncAccountsData.accounts_data.push(readAccountKeySignedPayload(value));
                break;
            case 2:
                syncAccountsData.incremental = value;
                break;
            case 3:
                syncAccountsData.requesting_full_sync = value;
                break;
            default:
                throw new Error(`Unsupported SyncAccountsData field number: ${fieldNumber}`);
        }
    });
}

// message AccountKeySignedPayload {
//     // protobuf-serialized AccountKeyPayload, required.
//     // It is passed in serialized form, because the protobuf encoding is non-deterministic.
//     // In particular encode(decode(payload)) might not match the signature.
//     bytes payload = 1;
//     // Signature of the payload, required.
//     Signature signature = 2;
//     // TODO: this is a good place to add optional fields: account_id, account_public_key,
//     // in case the signer of the message is not implied by the payload, or the context.
//     // Add them if needed.
//   }
function readAccountKeySignedPayload(data) {
    return readProto(data, (fieldNumber, value, accountKeySignedPayload) => {
        switch (fieldNumber) {
            case 1:
                accountKeySignedPayload.payload = value;
                break;
            case 2:
                accountKeySignedPayload.signature = value;
                break;
            default:
                throw new Error(`Unsupported AccountKeySignedPayload field number: ${fieldNumber}`);
        }
    });
}

// message Timestamp {
//     int64 seconds = 1;
//     int32 nanos = 2;
//   }
function readTimestamp(data) {
    return readProto(data, (fieldNumber, value, timestamp) => {
        switch (fieldNumber) {
            case 1:
                timestamp.seconds = value;
                break;
            case 2:
                timestamp.nanos = value;
                break;
            default:
                throw new Error(`Unsupported Timestamp field number: ${fieldNumber}`);
        }
    });
}

function writeTimestamp(timestamp) {
    const fields = [];
    if (timestamp.seconds !== undefined) {
        fields.push(writeProtoField(1, 0, timestamp.seconds));
    }
    if (timestamp.nanos !== undefined) {
        fields.push(writeProtoField(2, 0, timestamp.nanos));
    }

    return Buffer.concat(fields);
}

// Basic information about the chain view maintained by a peer.
// message PeerChainInfo {
//     GenesisId genesis_id = 1;
//     // Height of the highest NEAR chain block known to a peer.
//     uint64 height = 2;
//     // Shards of the NEAR chain tracked by the peer.
//     repeated uint64 tracked_shards = 3;
//     // Whether the peer is an archival node.
//     bool archival = 4;
//   }

function writePeerChainInfo(peerChainInfo) {
    const fields = [];
    if (peerChainInfo.genesis_id) {
        fields.push(writeProtoField(1, 2, writeGenesisId(peerChainInfo.genesis_id)));
    }
    if (peerChainInfo.height) {
        fields.push(writeProtoField(2, 0, peerChainInfo.height));
    }
    if (peerChainInfo.tracked_shards) {
        for (const trackedShard of peerChainInfo.tracked_shards) {
            fields.push(writeProtoField(3, 0, trackedShard));
        }
    }
    if (peerChainInfo.archival !== undefined) {
        fields.push(writeProtoField(4, 0, peerChainInfo.archival));
    }
    return Buffer.concat(fields);
}

// Unique identifier of the NEAR chain.
// message GenesisId {
//     // Name of the chain (for example "mainnet").
//     string chain_id = 1;
//     // Hash of the genesis block(?) of the NEAR chain.
//     CryptoHash hash = 2;
//   }

function writeGenesisId(genesisId) {
    const fields = [];
    if (genesisId.chain_id) {
        fields.push(writeProtoField(1, 2, Buffer.from(genesisId.chain_id)));
    }
    if (genesisId.hash) {
        // NOTE: this is a CryptoHash, but we don't have a type for it yet.
        fields.push(writeProtoField(2, 2, writeProtoField(1, 2, genesisId.hash)));
    }
    return Buffer.concat(fields);
}

// Response to Handshake, in case the Handshake was rejected.
// message HandshakeFailure {
//     enum Reason {
//       UNKNOWN = 0;
//       // Peer doesn't support protocol_version indicated in the handshake.
//       ProtocolVersionMismatch = 1;
//       // Peer doesn't belong to the chain indicated in the handshake.
//       GenesisMismatch = 2;
//       // target_id doesn't match the id of the peer.
//       InvalidTarget = 3;
//     }
//     // Reason for rejecting the Handshake.
//     Reason reason = 1;

//     // Data about the peer.
//     PeerInfo peer_info = 2;
//     // GenesisId of the NEAR chain that the peer belongs to.
//     GenesisId genesis_id = 3;
//     // Newest NEAR network version supported by the peer.
//     uint32 version = 4;
//     // Oldest NEAR network version supported by the peer.
//     uint32 oldest_supported_version = 5;
//   }
function readHandshakeFailure(data) {
    return readProto(data, (fieldNumber, value, handshakeFailure) => {
        switch (fieldNumber) {
            case 1:
                handshakeFailure.reason = value;
                break;
            case 2:
                // TODO
                // handshakeFailure.peerInfo = readPeerInfo(value);
                break;
            case 3:
                // TODO
                // handshakeFailure.genesisId = readGenesisId(value);
                break;
            case 4:
                handshakeFailure.version = value;
                break;
            case 5:
                handshakeFailure.oldestSupportedVersion = value;
                break;
            default:
                throw new Error(`Unsupported HandshakeFailure field number: ${fieldNumber}`);
        }
    });
}

// message RoutingTableUpdate {
//     reserved 3,4;
//     repeated Edge edges = 1;
//     // list of known NEAR validator accounts
//     repeated AnnounceAccount accounts = 2;
//   }

function readRoutingTableUpdate(data) {
    return readProto(data, (fieldNumber, value, routingTableUpdate) => {
        switch (fieldNumber) {
            case 1:
                if (!routingTableUpdate.edges) {
                    routingTableUpdate.edges = [];
                }
                routingTableUpdate.edges.push(readEdge(value));
                break;
            case 2:
                if (!routingTableUpdate.accounts) {
                    routingTableUpdate.accounts = [];
                }
                routingTableUpdate.accounts.push(readAnnounceAccount(value));
                break;
            default:
                throw new Error(`Unsupported RoutingTableUpdate field number: ${fieldNumber}`);
        }
    });
}

// Borsh-based messages

const { serialize, deserialize } = require('borsh');
const { BORSH_SCHEMA, Edge, EdgeInfo, AnnounceAccount, Block } = require('./network-borsh');
const { PublicKey } = require('./data-model');

function readWrappedBorsh(data, WrappedBorshClass) {
    let wrappedBorsh;
    readProto(data, (fieldNumber, value, _) => {
        switch (fieldNumber) {
            case 1:
                wrappedBorsh = deserialize(BORSH_SCHEMA, WrappedBorshClass, value);
                break;
            default:
                throw new Error(`Unsupported field number: ${fieldNumber}`);
        }
    });
    return wrappedBorsh;
}

function readPublicKey(data) {
    return readWrappedBorsh(data, PublicKey);
}

function writePublicKey(publicKey) {
    // NOTE: this is PublicKey borsh wrapper
    return writeProtoField(1, 2, serialize(BORSH_SCHEMA, publicKey));
}

function writePartialEdgeInfo(partialEdgeInfo) {
    // NOTE: this is PartialEdgeInfo borsh wrapper
    return writeProtoField(1, 2, serialize(BORSH_SCHEMA, partialEdgeInfo));
}

function readEdge(data) {
    return readWrappedBorsh(data, Edge);
}

function readAnnounceAccount(data) {
    return readWrappedBorsh(data, AnnounceAccount);
}

function readBlock(data) {
    return readWrappedBorsh(data, Block);
}

module.exports = { readPeerMessage, writePeerMessage };