-- Description: Remove all keys matching a pattern
-- Taken from https://chenriang.me/delete-keys-with-pattern-from-redis.html

local pattern= ARGV[1];

local cursor="0";

repeat
    local scanResult = redis.call("SCAN", cursor, "MATCH", pattern);
    local keys = scanResult[2];
    for i = 1, #keys do
        local key = keys[i];
        redis.call("DEL", key);
    end;
    cursor = scanResult[1];
until cursor == "0";

return "";
