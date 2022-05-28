class FastNEARError extends Error {
    code;
    data;

    constructor(code, message, data = {}) {
        super(message);
        this.code = code;
        this.data = data;
    }

    toString() {
        return `${this.code}: ${this.message}`;
    }
} 

module.exports = { FastNEARError };