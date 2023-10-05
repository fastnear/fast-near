function isJSON(buffer) {
    try {
        const MAX_WHITESPACE = 1000;
        const startSlice = buffer.slice(0, MAX_WHITESPACE + 1).toString('utf8').trim();
        if (/^\s*[\[\{"\d]/.test(startSlice)) {
            JSON.parse(buffer.toString('utf8'));
            return true;
        }
    } catch (e) {
        // Ignore error, means it's not valid JSON
    }

    return false;
}

module.exports = isJSON;