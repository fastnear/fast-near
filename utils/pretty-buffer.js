const textDecoder = new TextDecoder('utf8', { fatal: true });
const prettyBuffer = buffer => {
    try {
        return textDecoder.decode(buffer);
    } catch (e) {
        return buffer.toString('hex');
    }
};

module.exports = prettyBuffer;