const test = require('tape');

const isJSON = require('../utils/is-json');

// NOTE: Using async for sync tests removes the need to use t.plan or t.end

test('empty is not JSON', async t => t.false(isJSON(Buffer.from([]))));
test('binary is not JSON', async t => t.false(isJSON(Buffer.from([1, 2, 3]))));
test('quoted string is JSON', async t => t.true(isJSON(Buffer.from('"Hello, World!"'))));
test('quoted integer is JSON', async t => t.true(isJSON(Buffer.from('"12345"'))));
test('arbitrary integer is JSON', async t => t.true(isJSON(Buffer.from('12345'))));
test('arbitrary string is not JSON', async t => t.false(isJSON(Buffer.from('Hello, World!'))));
test('should detect JSON object', async t => t.true(isJSON(Buffer.from('{}'))));
test('should detect JSON object with trailing whitespace', async t => t.true(isJSON(Buffer.from('       {}   '))));
test('should detect JSON array', async t => t.true(isJSON(Buffer.from('[]'))));
test('should detect JSON array with trailing whitespace', async t => t.true(isJSON(Buffer.from('       []   '))));