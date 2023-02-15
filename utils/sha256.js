function sha256(data) {
    return require('crypto').createHash('sha256').update(data).digest();
}

module.exports = sha256;