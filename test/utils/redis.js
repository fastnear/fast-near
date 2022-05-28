const TEST_REDIS_PORT = 7123;
const TEST_REDIS_PID_FILE = 'test-redis.pid';
process.env.FAST_NEAR_REDIS_URL = process.env.FAST_NEAR_REDIS_URL || `redis://localhost:${TEST_REDIS_PORT}`;

const { spawn } = require('child_process');
const fs = require('fs');
const { closeRedis } = require('../../storage-client');

let redisProcess;
function startIfNeeded() {
    if (!fs.existsSync(TEST_REDIS_PID_FILE)) {
        console.log('Starting Redis')
        redisProcess = spawn('redis-server', [
            '--save', '',
            '--port', TEST_REDIS_PORT,
            '--pidfile', TEST_REDIS_PID_FILE
        ]);
    }
}

async function shutdown() {
    console.log('Shutting down');
    if (redisProcess) {
        console.log('Killing Redis');
        redisProcess.kill();
        redisProcess = null;
    }
    await closeRedis();
};

module.exports = { startIfNeeded, shutdown }