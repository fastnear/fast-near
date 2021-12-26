const redis = require('redis');
const nearAPI = require("near-api-js");

const { connect } = nearAPI;
const { keyStores } = nearAPI;

const config = {
  networkId: "mainnet",
  keyStore: new keyStores.InMemoryKeyStore(), // optional if not signing transactions
  nodeUrl: "https://rpc.mainnet.near.org",
  walletUrl: "https://wallet.mainnet.near.org",
  helperUrl: "https://helper.mainnet.near.org",
  explorerUrl: "https://explorer.mainnet.near.org",
};

const preload_account = process.argv[2];

(async () => {
  console.log('preloading contract code for account', preload_account);
  const near = await connect(config);
  const response = await near.connection.provider.query({
    request_type: "view_account",
    finality: "final",
    account_id: preload_account,
  });
  const block_height = response.block_height;
  const client = redis.createClient();
  await client.set('latest_block_height', block_height);
  const contractBlockHash = response.code_hash;
  await client.sendCommand('ZADD', [`code:${preload_account}`, block_height, response.code_hash]);
  await client.sendCommand('ZADD', [`account:${preload_account}`, block_height, response.block_hash]);

  const code_base64 = (await near.connection.provider.query({
    request_type: "view_code",
    block_id: block_height,
    account_id: preload_account,
  })).code_base64;
  await client.set(`code:${preload_account}:${contractBlockHash}`, Buffer.from(code_base64, 'base64'));

  console.log('getting data for', preload_account);
  try {
    const datavalues = (await near.connection.provider.query({
      request_type: "view_state",
      block_id: block_height,
      account_id: preload_account,
      prefix_base64: "",
    })).values;

    for (const datavalue of datavalues) {
      await client.sendCommand('ZADD', [`data:${preload_account}:${datavalue.key}`, block_height, datavalue.value]);
    }
  } catch (e) {
    console.error(e.message);
    if (e.message.indexOf('too large')) {
      console.log('trying to get state from history instead');
      const activityHistory = await (await fetch(`https://helper.mainnet.near.org/account/${preload_account}/activity`)).json();
      for(const activity of activityHistory) {
        const changes_response = await (await fetch(config.nodeUrl, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            jsonrpc: '2.0',
            id: 'dontcare',
            method: 'EXPERIMENTAL_changes', params: {
              "changes_type": "data_changes",
              "account_ids": [preload_account],
              "key_prefix_base64": "",
              "block_id": activity.block_hash
            }
          })
        })).text();
        console.log(changes_response);
      }
    }
  }
  await client.quit();
})();