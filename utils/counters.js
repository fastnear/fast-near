

let counters = {};

async function withTimeCounter(name, fn) {
    const startTime = performance.now();
    try {
        return await fn();
    } finally {
        const timeElapsed = performance.now() - startTime;
        counters[name] = (counters[name] || 0) + timeElapsed;
        counters[name + ':cnt'] = (counters[name  + ':cnt'] || 0) + 1;
    }
}

function resetCounters() {
    counters = {};
}

function getCounters() {
    return counters;
}

module.exports = { withTimeCounter, getCounters, resetCounters };