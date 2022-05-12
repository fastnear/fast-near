
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

module.exports = {
    Account,
    BORSH_SCHEMA,
};