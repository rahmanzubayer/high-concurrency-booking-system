-- KEYS[1]: item:{itemId}:inventory
-- KEYS[2]: item:{itemId}:booked_users
-- KEYS[3]: item:{itemId}:outbox        (Redis Stream)
-- KEYS[4]: item:{itemId}:soldout
-- ARGV[1]: userId
-- ARGV[2]: itemId
-- ARGV[3]: transactionId
-- ARGV[4]: event timestamp, epoch millis (supplied by the caller)
-- ARGV[5]: key TTL in seconds

-- 0. Is this sale open at all? A missing inventory key is not "sold out".
if redis.call('EXISTS', KEYS[1]) == 0 then
    return -2 -- Code -2: sale not warmed up / unknown item
end

-- 1. Has this user already secured a slot?
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
    return -1 -- Code -1: duplicate request
end

-- 2. Is there stock left?
local current_stock = tonumber(redis.call('GET', KEYS[1]))
if not current_stock or current_stock <= 0 then
    return 0 -- Code 0: sold out
end

-- 3. Take the slot AND record the event, in one atomic execution.
--    This is the transactional outbox: there is no window between them.
local remaining = redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
redis.call('XADD', KEYS[3], '*',
        'itemId', ARGV[2],
        'userId', ARGV[1],
        'transactionId', ARGV[3],
        'ts', ARGV[4])

-- 4. Make the sale clean up after itself.
local ttl = tonumber(ARGV[5])
redis.call('EXPIRE', KEYS[2], ttl)
redis.call('EXPIRE', KEYS[3], ttl)

-- 5. The moment the last slot goes, raise a flag the edge can read cheaply.
if remaining <= 0 then
    redis.call('SET', KEYS[4], '1', 'EX', ttl)
end

return 1 -- Code 1: slot secured
