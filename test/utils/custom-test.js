const test = require('tape');

function customTest(fn) {
    const result = function(...args) {
        fn(test, ...args);
    }
    result.only = function(...args) {
        fn(test.only, ...args);
    }
    return result;
}

module.exports = { customTest };