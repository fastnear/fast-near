const fetch = require('node-fetch');
const getRawBody = require('raw-body');

const { runContract }  = require('./run-contract');
const storage = require('./storage');
const { FastNEARError } = require('./error');

const { Account, BORSH_SCHEMA } = require('./data-model');

const { deserialize } = require('borsh');
const bs58 = require('bs58');
const resolveBlockHeight = require('./resolve-block-height');
const { accountKey } = require('./storage-keys');
const debug = require('debug')('json-rpc');

// NOTE: This is JSON-RPC proxy needed to pretend we are actual nearcore
const NODE_URL = process.env.FAST_NEAR_NODE_URL || 'https://rpc.mainnet.near.org';
const ARCHIVAL_NODE_URL = process.env.FAST_NEAR_ARCHIVAL_NODE_URL || 'https://rpc.mainnet.internal.near.org';

const proxyJson = async (ctx, { archival = false } = {}) => {
    const nodeUrl = archival ? ARCHIVAL_NODE_URL : NODE_URL;
    console.log('proxyJson', ctx.request.method, nodeUrl);
    const rawBody = ctx.request.body ? JSON.stringify(ctx.request.body) : await getRawBody(ctx.req);
    debug('proxyJson', ctx.request.method, ctx.request.path, rawBody.toString('utf8'));
    ctx.type = 'json';
    ctx.body = Buffer.from(await (await fetch(`${nodeUrl}${ctx.request.path}`, {
        method: ctx.request.method,
        headers: {
            'Content-Type': 'application/json'
        },
        body: ctx.request.method != 'GET' ? rawBody : undefined
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
        error: {
            code: -32000,
            data: message,
            message: "Server error",
        },
        id
    };
}

const ALWAYS_PROXY = ['yes', 'true'].includes((process.env.FAST_NEAR_ALWAYS_PROXY || 'no').trim().toLowerCase());

const handleError = async ({ ctx, blockHeight, error }) => {
    console.log('handleError', error);
    const { body } = ctx.request;
    const accountId = error.accountId;

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

const LRU = require('lru-cache');
const cache = new LRU({
    // TODO: Adjust cache size and max age
    max: 1000,
    // 1 second
    maxAge: 1000
});

const rpcResult = (id, result) => ({
    jsonrpc: '2.0',
    result,
    id
});

const withJsonRpcCache = async (ctx, next) => {
    const { body } = ctx.request;
    const cacheKey = JSON.stringify({ method: body.method, params: body.params });
    debug('cacheKey', cacheKey);
    let resultPromise = cache.get(cacheKey);
    let cacheHit = !!resultPromise;
    if (cacheHit) {
        debug('cache hit', cacheKey);
    }

    if (!resultPromise) {
        resultPromise = (async () => {
            await next();
            console.log('ctx.body', ctx.body);
            return ctx.body;
        })();
    }

    if (!cacheHit) {
        cache.set(cacheKey, resultPromise);
    }

    let resultBody = await resultPromise;
    console.log('cacheHit', cacheHit);
    console.log('resultBody', resultBody);
    if (!cacheHit) {
        ctx.type = 'json';
        ctx.body = resultBody;
        cache.set(cacheKey, resultBody);
    } else {
        if (Buffer.isBuffer(resultBody)) {
            resultBody = JSON.parse(resultBody.toString('utf8'));
        }

        const { result, error } = resultBody;
        ctx.body = { jsonrpc: '2.0', result, error, id: ctx.request.body.id };
    }
}

const parseJsonBody = async (ctx, next) => {
    ctx.request.body = JSON.parse((await getRawBody(ctx.req)).toString('utf8'));
    await next();
}

const handleJsonRpc = async ctx => {
    if (ALWAYS_PROXY) {
        return await proxyJson(ctx);
    }

    const { body } = ctx.request;
    try {
        if (body?.method == 'query') {
            const { finality, block_id } = body.params;
            // TODO: Determine proper way to handle finality. Depending on what indexer can do maybe just redirect to nearcore if not final

            if (typeof block_id == 'string') {
                // TODO: Maintain block hash -> block height mapping
                await proxyJson(ctx);
            } else {
                const blockHeight = await resolveBlockHeight(block_id);
                debug('blockHeight', blockHeight);

                const id = ctx.request.body.id;
                ctx.body = rpcResult(id, await handleQuery({ blockHeight, body }));
                return;
            }
        }

        await proxyJson(ctx);
    } catch (error) {
        await handleError({ ctx, blockHeight: null, error });
    }
};

async function handleQuery({ blockHeight, body }) {
    debug('handleQuery', blockHeight, body);

    if (body?.params?.request_type == 'call_function') {
        const { account_id, method_name: methodName, args_base64 } = body.params;
        return await callViewFunction({ blockHeight, accountId: account_id, methodName, args: Buffer.from(args_base64, 'base64') });
    }

    if (body?.params?.request_type == 'view_account') {
        const { account_id } = body.params;
        return await viewAccount({ blockHeight, accountId: account_id });
    }

    if (body?.params?.length) {
        const query = body.params[0];
        if (query?.startsWith('account/')) {
            const [, accountId] = query.split('/');
            return await viewAccount({ blockHeight, accountId });
        }

        if (query?.startsWith('call/')) {
            const [, accountId, methodName] = query.split('/');
            const args = bs58.decode(body.params[1], 'base64');
            return await callViewFunction({ blockHeight, accountId, methodName, args });
        }
    }
}

const callViewFunction = async ({ blockHeight, accountId, methodName, args }) => {
    const { result, logs, blockHeight: resolvedBlockHeight } = await runContract(accountId, methodName, args, blockHeight);
    const resultBuffer = Buffer.from(result);
    return {
        result: Array.from(resultBuffer),
        logs,
        block_height: parseInt(resolvedBlockHeight)
        // TODO: block_hash
    };
}

const viewAccount = async ({ blockHeight, accountId }) => {
    debug('find account data', accountId);
    const compKey = accountKey(accountId);
    const accountData = await storage.getLatestData(compKey, blockHeight);
    debug('account data loaded', accountId);
    if (!accountData) {
        throw new FastNEARError('accountNotFound', `Account not found: ${accountId} at ${blockHeight} block height`, { accountId, blockHeight });
    }

    const { amount, locked, code_hash, storage_usage } = deserialize(BORSH_SCHEMA, Account, accountData);
    return {
        amount: amount.toString(),
        locked: locked.toString(),
        code_hash: bs58.encode(code_hash),
        storage_usage: parseInt(storage_usage.toString()),
        block_height: parseInt(blockHeight)
        // TODO: block_hash
    };
}

module.exports = { parseJsonBody, withJsonRpcCache, handleJsonRpc, proxyJson };