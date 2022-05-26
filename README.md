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

## Pulling data into Redis

To load from NEAR Lake (use `--help` to learn more about options):

```
node scripts/load-from-near-lake.js near-lake-data-mainnet --batch-size 50 --history-length 1
```

See also https://github.com/vgrichina/near-state-indexer for Rust implementation running proper nearcore node.


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

