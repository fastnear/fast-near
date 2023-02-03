const prettyBuffer = buffer => {
    return Array.from(buffer).map(c => {
        if (c >= 32 && c <= 126) {
            return String.fromCharCode(c);
        } else {
            return `\\x${c.toString(16).padStart(2, '0')}`;
        }
    }).join('');
};

module.exports = prettyBuffer;