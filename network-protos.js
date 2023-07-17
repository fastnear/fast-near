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
                // handshake_failure
                result.handshakeFailure = readHandshakeFailure(value);
                break;
            case 6:
                // last_edge
                result.lastEdge = readLastEdge(value);
                break;
            case 7:
                // sync_routing_table
                result.syncRoutingTable = readSyncRoutingTable(value);
                break;
            case 8:
                // update_nonce_request
                result.updateNonceRequest = readUpdateNonceRequest(value);
                break;
            case 9:
                // update_nonce_response
                result.updateNonceResponse = readUpdateNonceResponse(value);
                break;
            case 10:
                // peers_request
                result.peersRequest = readPeersRequest(value);
                break;
            case 11:
                // peers_response
                result.peersResponse = readPeersResponse(value);
                break;
            case 12:
                // block_headers_request
                result.blockHeadersRequest = readBlockHeadersRequest(value);
                break;
            case 13:
                // block_headers_response
                result.blockHeadersResponse = readBlockHeadersResponse(value);
                break;
            case 14:
                // block_request
                result.blockRequest = readBlockRequest(value);
                break;
            case 15:
                // block_response
                result.blockResponse = readBlockResponse(value);
                break;
            case 16:
                // transaction
                result.transaction = readSignedTransaction(value);
                break;
            case 17:
                // routed
                result.routed = readRoutedMessage(value);
                break;
            case 18:
                // disconnect
                result.disconnect = readDisconnect(value);
                break;
            case 19:
                // challenge
                result.challenge = readChallenge(value);
                break;
            case 25:
                // sync_accounts_data
                result.syncAccountsData = readSyncAccountsData(value);
                break;
            case 26:
                // trace_context
                result.traceContext = readTraceContext(value);
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
        // tier2_handshake
        fields.push(writeProtoField(4, writeHandshake(peerMessage.handshake)));
    }
    if (peerMessage.handshakeFailure) {
        fields.push(writeProtoField(5, writeHandshakeFailure(peerMessage.handshakeFailure)));
    }
    if (peerMessage.lastEdge) {
        fields.push(writeProtoField(6, writeLastEdge(peerMessage.lastEdge)));
    }
    if (peerMessage.syncRoutingTable) {
        fields.push(writeProtoField(7, writeSyncRoutingTable(peerMessage.syncRoutingTable)));
    }
    if (peerMessage.updateNonceRequest) {
        fields.push(writeProtoField(8, writeUpdateNonceRequest(peerMessage.updateNonceRequest)));
    }
    if (peerMessage.updateNonceResponse) {
        fields.push(writeProtoField(9, writeUpdateNonceResponse(peerMessage.updateNonceResponse)));
    }
    if (peerMessage.peersRequest) {
        fields.push(writeProtoField(10, writePeersRequest(peerMessage.peersRequest)));
    }
    if (peerMessage.peersResponse) {
        fields.push(writeProtoField(11, writePeersResponse(peerMessage.peersResponse)));
    }
    if (peerMessage.blockHeadersRequest) {
        fields.push(writeProtoField(12, writeBlockHeadersRequest(peerMessage.blockHeadersRequest)));
    }
    if (peerMessage.blockHeadersResponse) {
        fields.push(writeProtoField(13, writeBlockHeadersResponse(peerMessage.blockHeadersResponse)));
    }
    if (peerMessage.blockRequest) {
        fields.push(writeProtoField(14, writeBlockRequest(peerMessage.blockRequest)));
    }
    if (peerMessage.blockResponse) {
        fields.push(writeProtoField(15, writeBlockResponse(peerMessage.blockResponse)));
    }
    if (peerMessage.transaction) {
        fields.push(writeProtoField(16, writeSignedTransaction(peerMessage.transaction)));
    }
    if (peerMessage.routed) {
        fields.push(writeProtoField(17, writeRoutedMessage(peerMessage.routed)));
    }
    if (peerMessage.disconnect) {
        fields.push(writeProtoField(18, writeDisconnect(peerMessage.disconnect)));
    }
    if (peerMessage.challenge) {
        fields.push(writeProtoField(19, writeChallenge(peerMessage.challenge)));
    }
    if (peerMessage.syncAccountsData) {
        fields.push(writeProtoField(25, writeSyncAccountsData(peerMessage.syncAccountsData)));
    }
    if (peerMessage.traceContext) {
        fields.push(writeProtoField(26, writeTraceContext(peerMessage.traceContext)));
    }
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
                handshake.protocolVersion = value;
                break;
            case 2:
                handshake.oldestSupportedVersion = readUint32(value);
                break;
            case 3:
                handshake.senderPeerId = readPublicKey(value);
                break;
            case 4:
                handshake.targetPeerId = readPublicKey(value);
                break;
            case 5:
                handshake.senderListenPort = value;
                break;
            case 6:
                handshake.senderChainInfo = readPeerChainInfo(value);
                break;
            case 7:
                handshake.partialEdgeInfo = readPartialEdgeInfo(value);
                break;
            case 8:
                handshake.ownedAccount = readAccountKeySignedPayload(value);
                break;
            default:
                throw new Error(`Unsupported Handshake field number: ${fieldNumber}`);
        }
    });
    return handshake;
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

module.exports = { readPeerMessage, writePeerMessage };