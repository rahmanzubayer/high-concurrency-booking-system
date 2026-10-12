-- KEYS[1]: item:{itemId}:inventory
-- KEYS[2]: item:{itemId}:booked_users
-- ARGV[1]: userId

-- 0. Is this sale actually open? A missing inventory key is not "sold out".
if redis.call('EXISTS', KEYS[1]) == 0 then
    return -2 -- Code -2: sale not warmed up / unknown item
end

-- 1. Check if the user has already secured a slot
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
    return -1 -- Code -1: User already purchased
end

-- 2. Check current inventory stock
local current_stock = tonumber(redis.call('GET', KEYS[1]))

if not current_stock or current_stock <= 0 then
    return 0 -- Code 0: Out of stock
end

-- 3. Atomically decrement stock and record the user
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])

return 1 -- Code 1: Success! Slot secured
