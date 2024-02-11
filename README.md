[![Coverage Status](https://coveralls.io/repos/github/vgrichina/fast-near/badge.svg?branch=main)](https://coveralls.io/github/vgrichina/fast-near?branch=main)

# What is fast-near?

fast-near aims to provide the fastest RPC implementation for @NEARProtocol using high performance storage backends like:
 - in-memory storage in Redis.
- SSD optimized storage using LMDB.

It is optimized for view call performance and ease of deploy and scaling. 

It currently doesn't sync with network on its own, data needs to be loaded either from [NEAR Lake](https://github.com/near/near-lake-indexer) or from https://github.com/vgrichina/near-state-indexer.

# Why

`nearcore` RPC performance isn't good enough for novel use cases like https://web4.near.page.

fast-near achieves better performance by using
- in-memory storage using Redis
- client-side caching to save on Redis I/O
- V8 WebAssembly implementation
- disabled gas metering (timeout works fine for view calls)
- simpler REST API (no JSON wrapper if passing large binaries, etc)
- good compatibility with caching at HTTP layer (using Nginx, etc)

fast-near is also a good fit if you want to run RPC node serving limited subset of accounts (e.g. supporting your app exclusively) on a smaller hardware. This works well if data is sourced from [NEAR Lake](https://github.com/near/near-lake-indexer).

# How to

## Run directly from npm:

```
FAST_NEAR_REDIS_URL=<redis_ip> FAST_NEAR_NODE_URL=<rpc_endpoint> npx fast-near
```

## Build and run via yarn:

```
yarn
FAST_NEAR_REDIS_URL=<redis_ip> FAST_NEAR_NODE_URL=<rpc_endpoint> yarn start
```

## Build and run with docker:

```
docker build -t fastrpc .
docker run -d -e FAST_NEAR_REDIS_URL=<redis_ip> -e FAST_NEAR_NODE_URL=<rpc_endpoint> fastrpc
```

## Pull data into Redis

To load from NEAR Lake (use `--help` to learn more about options):

```
node scripts/load-from-near-lake.js near-lake-data-mainnet --batch-size 50 --history-length 1 --dump-changes
```

## Pull data into LMDB (experimental)

To load from NEAR Lake (use `--help` to learn more about options):

```bash
FAST_NEAR_STORAGE_TYPE=lmdb node scripts/load-from-near-lake.js near-lake-data-mainnet --batch-size 50 --history-length 1 --dump-changes
```

Run server:

```bash
FAST_NEAR_STORAGE_TYPE=lmdb yarn start
```

## Load data only for your app's smart contracts

To load data for `app1.near`, `app2.near` and all subaccounts of `superapp.near`:

```
node scripts/load-from-near-lake.js near-lake-data-mainnet --include app1.near --include app2.near --include '*.superapp.near' --dump-changes
```

## Exclude some undesired accounts

To load data for all accounts except `aurora` and `sweat` subaccounts:

```
node scripts/load-from-near-lake.js near-lake-data-mainnet --exclude aurora.* --exclude sweat.* --dump-changes
```

## Different data sink options

Currently there are such options to dump data loaded from NEAR Lake:
- `--dump-changes` - dumps state changes into storage. Use `FAST_NEAR_STORAGE_TYPE` to specify storage type. Defaults to `redis`
- `--dump-questdb` - dumps receipts into QuestDB (https://questdb.io/)
- `--dump-estuary` - dumps blocks into IPFS using Estuary (https://estuary.tech/)

## Load data from a local node

See https://github.com/vgrichina/near-state-indexer for Rust implementation running full nearcore node.

# CLI options

## Environment variables

- `PORT` - port to listen on (default: `3000`)
- `ESTUARY_TOKEN` - token to use for Estuary IPFS upload. See https://docs.estuary.tech/tutorial-get-an-api-key for more information. 
- `FAST_NEAR_STORAGE_TYPE` - storage type to use (default: `redis`). Supported values: `redis`, `lmdb`.
- `FAST_NEAR_ENABLE_CACHE` - enable client-side caching (default: `true`).
- `FAST_NEAR_LMDB_PATH` - path to LMDB database (default: `./lmdb-data`). This is only used if `FAST_NEAR_STORAGE_TYPE` is set to `lmdb`.
- `FAST_NEAR_REDIS_URL` - Redis URL (default: `redis://localhost:6379`)
- `FAST_NEAR_QUESTDB_URL` - QuestDB URL (default: `http://localhost:9000`)
- `FAST_NEAR_NODE_URL` - NEAR RPC endpoint (default: `https://rpc.mainnet.near.org`). This is only used as a fallback for JSON-RPC endpoint.
- `FAST_NEAR_ARCHIVAL_NODE_URL` - NEAR RPC endpoint for archival node (default: `https://rpc.mainnet.internal.near.org`). This is only used as a fallback for JSON-RPC endpoint for data unavailable in Redis or on non-archival RPC.
- `FAST_NEAR_ALWAYS_PROXY` - Always proxy JSON-RPC requests to `FAST_NEAR_NODE_URL` (default: `false`).
- `FAST_NEAR_START_BLOCK_HEIGHT` - Minimum block height expected to be present in Redis (default: `0`).
- `FAST_NEAR_WORKER_COUNT` - Number of workers to use for execution of WASM code. (default: `4`).
- `FAST_NEAR_CONTRACT_TIMEOUT_MS` - Timeout for contract execution in milliseconds (default: `1000`).

# HTTP API

## Public endpoints

- Testnet: https://rpc.web4.testnet.page/account/testnet
- Mainnet: https://rpc.web4.near.page/account/near

## Call view method

### POST

You can post either JSON or binary body, it's passed raw as input to given method.


URL format:

```
https://rpc.web4.near.page/account/<contract_account_id>/view/<method_name>
```

#### Examples

```
http post https://rpc.web4.near.page/account/vlad.tkn.near/view/ft_balance_of account_id=vlad.near
```

### GET

Parameters are passed as part of URL query.

URL format:

```
https://rpc.web4.near.page/account/<contract_account_id>/view/<method_name>?<arg_name>=<string_arg_value>&<arg_name.json>=<json_arg_value>
```

#### Examples

##### String parameters:

```
curl 'https://rpc.web4.near.page/account/vlad.tkn.near/view/ft_balance_of?account_id=vlad.near'
```

[https://rpc.web4.near.page/account/vlad.tkn.near/view/ft_balance_of?account_id=vlad.near](https://rpc.web4.near.page/account/vlad.tkn.near/view/ft_balance_of?account_id=vlad.near)


##### JSON parameters:

```
curl --globoff 'https://rpc.web4.near.page/account/lands.near/view/web4_get?request.json={"path":"/"}'
```

[https://rpc.web4.near.page/account/lands.near/view/web4_get?request.json={"path":"/"}](https://rpc.web4.near.page/account/lands.near/view/web4_get?request.json={"path":"/"})


##### Number parameters (passed as JSON):


```
curl 'https://rpc.web4.near.page/account/lands.near/view/getChunk?x.json=0&y.json=0'
```

[https://rpc.web4.near.page/account/lands.near/view/getChunk?x.json=0&y.json=0](https://rpc.web4.near.page/account/lands.near/view/getChunk?x.json=0&y.json=0)

## Get account info

### GET

URL format:

```
https://rpc.web4.near.page/account/<account_id>
```

#### Example


```
curl 'https://rpc.web4.near.page/account/vlad.near'
```

[https://rpc.web4.near.page/account/vlad.near](https://rpc.web4.near.page/account/vlad.near)


## Get access key info

### GET

URL format:

```
https://rpc.web4.near.page/account/<account_id>/key/<public_key>
```

#### Example


```
curl 'https://rpc.web4.near.page/account/vlad.near/key/ed25519:JBHUrhF61wfScUxqGGRmfdJTQYg8MzRr5H8pqMMjqygr'
```

[https://rpc.web4.near.page/account/vlad.near/key/ed25519:JBHUrhF61wfScUxqGGRmfdJTQYg8MzRr5H8pqMMjqygr](https://rpc.web4.near.page/account/vlad.near/key/ed25519:JBHUrhF61wfScUxqGGRmfdJTQYg8MzRr5H8pqMMjqygr)


## Download contract WASM code

### GET

URL format:

```
https://rpc.web4.near.page/account/<account_id>/contract
```

#### Example


```
curl 'https://rpc.web4.near.page/account/vlad.tkn.near/contract'
```

[https://rpc.web4.near.page/account/vlad.tkn.near/contract](https://rpc.web4.near.page/account/vlad.tkn.near/contract)


## Get contract methods list

### GET

URL format:

```
https://rpc.web4.near.page/account/<account_id>/contract/methods
```

#### Example


```
curl 'https://rpc.web4.near.page/account/lands.near/contract/methods'
```

[https://rpc.web4.near.page/account/lands.near/contract/methods](https://rpc.web4.near.page/account/lands.near/contract/methods)


# Roadmap

Some of the planned and already implemented components. Is not exhaustive list.

- Loading data
    - [x] Allow loading from NEAR Data Lake
    - [ ] Allow loading from [NEAR ZeroMQ Indexer](https://github.com/here-wallet/near-zmq-indexer) for smaller latency
    - [x] Compress history to given time window
    - [?] Update near-state-indexer to load latest format in Redis
    - [?] Update nearcore to load latest format in Redis
    - [x] Load account keys
    - [x] Filter accounts when loading
    - [ ] Load recent transactions results
    - [ ] Manage lowest known block height dynamically
    - [ ] Delegate to another fast-near REST API instance if given account data not present 
    - [ ] Delegate to another nearcore JSON-RPC instance if given account data not present?
- REST API
    - [x] Call view methods
    - [x] View contract WASM
    - [x] View contract methods
    - [x] View account
    - [x] View contract state
    - [x] View account access key
    - [ ] View account access keys list
    - [ ] View transaction results
    - [ ] Submit transaction
    - [ ] Structured error handling
- JSON-RPC API
    - [x] Call view methods
    - [x] View account
    - [x] Proxy to another node if not implemented / hitting archival
    - [ ] Decide whether needs to be supported (e.g. Pagoda can allocate grant)
- NEAR P2P Protocol
    - [x] Basic data structures
    - [x] POC downloading blocks with transactions
    - [ ] Submit transaction
    - [ ] Load and execute transactions
- WASM Runtime
    - [x] Basic view method support
    - [ ] Implement missing imports for view methods
    - [ ] State change method support
- Storage
    - [x] Redis
    - [x] Abstract storage API
    - [x] LMDB storage
    - [ ] Load storage selectively from another fast-near instance
    - [ ] Browser-based storage
- Tests
    - [x] Test compress-history
    - [x] Test view calls
    - [x] Integration test with loading near-lake mainnet data
    - [ ] Full coverage of runtime methods
    - [ ] More robust integration tests
