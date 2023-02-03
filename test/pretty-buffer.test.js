const test = require('tape');

const prettyBuffer = require('../utils/pretty-buffer');

// NOTE: Using async for sync tests removes the need to use t.plan or t.end

test('empty buffer', async t => t.equal(prettyBuffer(Buffer.from([])), ''));
test('binary', async t => t.equal(prettyBuffer(Buffer.from([1, 2, 3])), '\\x01\\x02\\x03'));
test('ascii', async t => t.equal(prettyBuffer(Buffer.from('Hello, World!')), 'Hello, World!'));
test('ascii and binary', async t => t.equal(prettyBuffer(Buffer.from('Hello, World!\\x01\\x02\\x03')), 'Hello, World!\\x01\\x02\\x03'));
test('invalid utf-8', async t => t.equal(prettyBuffer(Buffer.from([0xFF, 0xFF, 0xFF])), '\\xff\\xff\\xff'));