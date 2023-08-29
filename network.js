
const assert = require('assert');
const { serialize, deserialize } = require('borsh');
const { PublicKey } = require('./data-model');
const { BORSH_SCHEMA, PingPong, RoutedMessageToSign, RoutedMessageBody, RoutedMessage, StateRequestHeader, PeerIdOrHash, PeerChainInfoV2, GenesisId, EdgeInfo, EdgeInfoToSign, Signature, PartialEncodedChunkRequestMsg } = require('./network-borsh'); 
const { readPeerMessage, writePeerMessage } = require('./network-protos');

const ed = require('@noble/ed25519');
const sha256  = require('./utils/sha256');

const privateKey = ed.utils.randomPrivateKey(); // 32-byte Uint8Array or string.

const signObject = async (obj) => {
    const data = serialize(BORSH_SCHEMA, obj);
    const signature = await ed.sign(sha256(data), privateKey);
    return new Signature({ keyType: 0, data: Buffer.from(signature) });
}

const signEdgeInfo = async (nonce, peer0, peer1) => {
    if (Buffer.compare(peer0.ed25519.data, peer1.ed25519.data) > 0) {
        [peer1, peer0] = [peer0, peer1];
    }
    return signObject(new EdgeInfoToSign({ peer0, peer1, nonce }));
}

const bs58 = require('bs58');
const net = require('net');
const EventEmitter = require('events');
const { Buffer } = require('buffer');

const sendMessage = (socket, message) => {
    const messageData = writePeerMessage(message);
    const length = Buffer.alloc(4);
    length.writeInt32LE(messageData.length, 0);

    console.log('messageData', messageData.toString('hex'));
    socket.write(Buffer.concat([length, messageData]));
}

if (!process.env.TARGET_PEER_ID) {
    console.error('TARGET_PEER_ID is not set');
    process.exit(1);
}
const target_peer_id = PublicKey.fromString(process.env.TARGET_PEER_ID);
let sender_peer_id;

const NODE_ADDRESS = process.env.NODE_ADDRESS || '127.0.0.1'
const NUM_TOTAL_PARTS = 50;
const NUM_DATA_PARTS = Math.floor((NUM_TOTAL_PARTS - 1) / 3);

const socket = net.connect(24567, NODE_ADDRESS, async () => {
    console.log('connected');

    const publicKey = Buffer.from(await ed.getPublicKey(privateKey));
    const nonce = 1;
    sender_peer_id = PublicKey.fromString('ed25519:' + bs58.encode(publicKey));
    console.log('peer_id', bs58.encode(publicKey));

    const handshake = {
        handshake: {
            protocol_version: 61,
            oldest_supported_version: 60,
            sender_peer_id,
            target_peer_id,
            sender_listen_port: 24567,
            sender_chain_info: {
                genesis_id: {
                    chain_id: 'mainnet',
                    hash: bs58.decode('EPnLgE7iEq9s7yTkos96M3cWymH5avBAPm3qx3NXqR8H')
                },
                height: 95000000, // TODO: Update to avoid HeightFraud?
                tracked_shards: [0],
                archival: 0
            },
            partial_edge_info: new EdgeInfo({
                nonce,
                signature: await signEdgeInfo(nonce, sender_peer_id, target_peer_id),
            })
        }
    };
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

        const message = readPeerMessage(data.slice(4, 4 + length));
        console.log('message', message);

        // const message = deserialize(BORSH_SCHEMA, PeerMessage, data.slice(4, 4 + length));
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
        author: sender_peer_id,
        body: new RoutedMessageBody(messageBodyObj)
    });
    sendMessage(socket, {
        routed: {
            borsh: serialize(BORSH_SCHEMA, new RoutedMessage({
                target: messageToSign.target,
                author: messageToSign.author,
                signature: await signObject(messageToSign),
                ttl: 100,
                body: messageToSign.body
            })),
            created_at: {
                nanos: Math.floor(Date.now() * 1000000),
            },
            num_hops: 0,
        }
    });
}

const chunkHash = (chunkHeader) => {
    // TODO: v1?
    if (chunkHeader.v2) {
        const { inner } = chunkHeader.v2;
        const serialized = serialize(BORSH_SCHEMA, inner);
        const innerHash = sha256(serialized);
        return sha256(Buffer.concat([innerHash, inner.encoded_merkle_root]));
    }

    if (chunkHeader.v3) {
        // TODO: Support other versions of inner?
        const inner = chunkHeader.v3.inner.v2;
        const serialized = serialize(BORSH_SCHEMA, inner);
        const innerHash = sha256(serialized);
        return sha256(Buffer.concat([innerHash, inner.encoded_merkle_root]));
    }
}

const chunkHeaders = {};

eventEmitter.on('message', async message => {
    if (message.handshake) {
        console.log('received handshake');

        // sendMessage(socket, new PeerMessage({ peers_request: new PeersRequest() }));
        // sendMessage(socket, new PeerMessage({
        //     // NOTE: block is returned by previous block hash?
        //     block_request: new BlockRequest({ block_hash: bs58.decode('5xFjQhte3amNExUZxtLRaKEzQ7qofStXm5WjU63sjhM4') })
        // }));

        sendRoutedMessage({
            ping: new PingPong({ nonce: 0, source: sender_peer_id })
        });

        sendRoutedMessage({
            state_request_header: new StateRequestHeader({
                shard_id: 0,
                // NOTE: Should this be prev hash of epoch start block hash?
                hash: bs58.decode('2i88qoGUT1HJ7rQoc5QrR492K1tpAKbEyyRF7h3YS6zS')
            })
        });
    }

    if (message.block_response) {
        console.log('block_response', message.block_response);

        // TODO: Adjust following
        const header = message.block_response.block.v2.header.v3;
        console.log('header', message.block_response.block.v2.header);
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

        const { chunks } = message.block_response.block.v2;

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
