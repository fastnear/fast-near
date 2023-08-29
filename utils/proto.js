function readVarint(data, offset) {
    let value = 0;
    let shift = 0;
    while (true) {
        const byte = data[offset++];
        value |= (byte & 0x7f) << shift;
        if (byte < 0x80) {
            return [value, offset];
        }
        shift += 7;
    }
}

function writeVarint(value) {
    const buffer = Buffer.alloc(10);
    let offset = 0;
    while (true) {
        const byte = value & 0x7f;
        value >>= 7;
        if (value === 0) {
            buffer[offset++] = byte;
            return buffer.subarray(0, offset);
        }
        buffer[offset++] = byte | 0x80;
    }
}

function readProto(data, processField) {
    console.log('readProto', data);
    const result = {};
    let offset = 0;
    while (offset < data.length) {
        let fieldTag;
        [fieldTag, offset] = readVarint(data, offset);
        const fieldNumber = fieldTag >> 3;
        const wireType = fieldTag & 0x7;
        let value;
        switch (wireType) {
            case 0:
                // Varint
                [value, offset] = readVarint(data, offset);
                break;
            case 1:
                // 64-bit
                value = data.readBigUInt64LE(offset);
                offset += 8;
                break;
            case 2: {
                // Length-delimited
                let length;
                [length, offset] = readVarint(data, offset);
                value = data.subarray(offset, offset + length);
                offset += length;
                break;
            }
            default:
                throw new Error(`Unsupported wire type: ${wireType}`);
        }
        processField(fieldNumber, value, result);
    }
    return result;
}

function writeProtoField(fieldNumber, wireType, value) {
    const fieldTag = (fieldNumber << 3) | wireType;
    const fieldTagBytes = writeVarint(fieldTag);
    switch (wireType) {
        case 0:
            // Varint
            return Buffer.concat([fieldTagBytes, writeVarint(value)]);
        case 1: {
            // 64-bit
            const buffer = Buffer.alloc(8);
            buffer.writeBigInt64LE(value);
            return Buffer.concat([fieldTagBytes, buffer]);
        }
        case 2: {
            // Length-delimited
            const buffer = Buffer.from(value);
            return Buffer.concat([fieldTagBytes, writeVarint(buffer.length), buffer]);
        }
        default:
            throw new Error(`Unsupported wire type: ${wireType}`);
    }
}

module.exports = { readProto, writeProtoField };