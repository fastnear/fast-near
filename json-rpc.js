const fetch = require('node-fetch');
const getRawBody = require('raw-body');

const runContract  = require('./run-contract');
const storageClient = require('./storage-client');
const { FastNEARError } = require('./error');

class Account {
    amount;
    locked;
    code_hash;
    storage_usage;

    constructor(args) {
        Object.assign(this, args);
    }
}

const BORSH_SCHEMA = new Map([
    [Account, {
        kind: 'struct',
        fields: [
            ['amount', 'u128'],
            ['locked', 'u128'],
            ['code_hash', ['u8', 32]],
            ['storage_usage', 'u64'],
        ]
    }]
]);

const { deserialize } = require('borsh');
const bs58 = require('bs58');
const resolveBlockHeight = require('./resolve-block-height');
const debug = require('debug')('json-rpc');

// NOTE: This is JSON-RPC proxy needed to pretend we are actual nearcore
const NODE_URL = process.env.FAST_NEAR_NODE_URL || 'https://rpc.mainnet.near.org';
const ARCHIVAL_NODE_URL = process.env.FAST_NEAR_ARCHIVAL_NODE_URL || 'https://rpc.mainnet.internal.near.org';

const proxyJson = async (ctx, { archival = false }) => {
    const nodeUrl = archival ? ARCHIVAL_NODE_URL : NODE_URL;
    const rawBody = ctx.request.body ? JSON.stringify(ctx.request.body) : await getRawBody(ctx.req);
    debug('proxyJson', ctx.request.method, ctx.request.path, rawBody.toString('utf8'));
    ctx.type = 'json';
    ctx.body = Buffer.from(await (await fetch(`${nodeUrl}${ctx.request.path}`, {
        method: ctx.request.method,
        headers: {
            'Content-Type': 'application/json'
        },
        body: rawBody
    })).arrayBuffer());
}

const viewCallError = ({ id, message }) => {
    return {
        jsonrpc: '2.0',
        result: {
            error: message,
            // TODO: block_height and block_hash
        },
        id
    };
}

const legacyError = ({ id, message }) => {
    return {
        jsonrpc: '2.0',
        // TODO: Structured error in addition to legacy?
        result: {
            code: -32000,
            data: message,
            message: "Server error",
        },
        id
    };
}

const ALWAYS_PROXY = ['yes', 'true'].includes((process.env.FAST_NEAR_ALWAYS_PROXY || 'no').trim().toLowerCase());

const handleError = async ({ ctx, accountId, blockHeight, error }) => {
    const { body } = ctx.request;

    // TODO: Match error handling? Structured errors? https://docs.near.org/docs/api/rpc/contracts#what-could-go-wrong-6
    const message = error.toString();
    if (/TypeError.* is not a function/.test(message)) {
        ctx.body = viewCallError({
            id: body.id,
            message: "wasm execution failed with error: FunctionCallError(MethodResolveError(MethodNotFound))"
        });
        return;
    }

    switch (error.code) {
    case 'notImplemented':
        await proxyJson(ctx);
        return;
    case 'panic':
    case 'abort':
        ctx.body = viewCallError({
            id: body.id,
            message: `wasm execution failed with error: FunctionCallError(HostError(GuestPanic { panic_msg: ${JSON.stringify(error.message)}}))`
        });
        return;
    case 'codeNotFound':
        ctx.body = viewCallError({
            id: body.id,
            message: `wasm execution failed with error: FunctionCallError(CompilationError(CodeDoesNotExist { account_id: AccountId("${accountId}") }))`
        });
        return;
    case 'blockHeightTooLow':
        await proxyJson(ctx, { archival: true });
        return;
    case 'blockHeightTooHigh':
        ctx.body = legacyError({
            id: body.id,
            message: `DB Not Found Error: BLOCK HEIGHT: ${blockHeight} \n Cause: Unknown`
        });
        return;
    case 'accountNotFound':
        ctx.body = legacyError({
            id: body.id,
            message: `account ${accountId} does not exist while viewing`
        });
        return;
    }

    ctx.throw(400, message);
}

const parseBlockIndex = async (ctx) => {
    const { finality, block_id } = ctx.request.body.params;
    // TODO: Determine proper way to handle finality. Depending on what indexer can do maybe just redirect to nearcore if not final

    if (typeof block_id == 'string') {
        // TODO: Maintain block hash -> block height mapping
        await proxyJson(ctx);
        return false;
    }

    ctx.blockHeight = block_id;
    return true;
}

const handleJsonRpc = async ctx => {
    if (ALWAYS_PROXY) {
        return await proxyJson(ctx, {});
    }

    ctx.request.body = JSON.parse((await getRawBody(ctx.req)).toString('utf8'));

    const { body } = ctx.request;
    if (body?.method == 'query' && body?.params?.request_type == 'call_function') {
        const { account_id: accountId, method_name: methodName, args_base64 } = body.params;
        await parseBlockIndex(ctx);
        await callViewFunction(ctx, { accountId, methodName, args: Buffer.from(args_base64, 'base64') });
        return;
    }

    if (body?.method == 'query' && body?.params?.request_type == 'view_account') {
        const { account_id: accountId } = body.params;
        await parseBlockIndex(ctx);
        await viewAccount(ctx, { accountId });
        return;
    }

    if (body?.method == 'query' && body?.params?.length) {
        const query = body.params[0];
        if (query?.startsWith('account/')) {
            const [, accountId] = query.split('/');
            await viewAccount(ctx, { accountId });
            return;
        }

        if (query?.startsWith('call/')) {
            const [, accountId, methodName] = query.split('/');
            const args = bs58.decode(body.params[1], 'base64');
            await callViewFunction(ctx, { accountId, methodName, args });
            return;
        }
    }

    await proxyJson(ctx);
};

const callViewFunction = async (ctx,  { accountId, methodName, args }) => {
    const { blockHeight } = ctx;
    try {
        const { result, logs, blockHeight: resolvedBlockHeight } = await runContract(accountId, methodName, args, blockHeight);
        const resultBuffer = Buffer.from(result);
        ctx.body = {
            jsonrpc: '2.0',
            result: {
                result: Array.from(resultBuffer),
                logs,
                block_height: parseInt(resolvedBlockHeight)
                // TODO: block_hash
            },
            id: ctx.request.body.id
        };
        return;
    } catch (error) {
        await handleError({ ctx, accountId, blockHeight, error });
    }
}

const viewAccount = async (ctx, { accountId }) => {
    let { blockHeight } = ctx;
    try {
        blockHeight = await resolveBlockHeight(blockHeight);
        debug('blockHeight', blockHeight);

        debug('find account data', accountId);
        const blockHash = await storageClient.getLatestAccountBlockHash(accountId, blockHeight);
        debug('blockHash', blockHash);
        if (!blockHash) {
            throw new FastNEARError('accountNotFound', `Account not found: ${accountId} at ${blockHeight} block height`);
        }

        const accountData = await storageClient.getAccountData(accountId, blockHash);
        debug('account data loaded', accountId);
        if (!accountData) {
            throw new FastNEARError('accountNotFound', `Account not found: ${accountId} at ${blockHeight} block height`);
        }

        const { amount, locked, code_hash, storage_usage } = deserialize(BORSH_SCHEMA, Account, accountData);
        ctx.body = {
            jsonrpc: '2.0',
            result: {
                amount: amount.toString(),
                locked: locked.toString(),
                code_hash: bs58.encode(code_hash),
                storage_usage: parseInt(storage_usage.toString()),
                block_height: parseInt(blockHeight)
                // TODO: block_hash
            },
            id: ctx.request.body.id
        };
    } catch (error) {
        await handleError({ ctx, accountId: accountId, blockHeight, error });
    }
}

module.exports = { handleJsonRpc, proxyJson };