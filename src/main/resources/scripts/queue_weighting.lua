--[[
    Weighted Queue Processing Lua Script

    Atomically polls items from multiple queues based on configurable weights
    and priority order, reducing Redis round-trips from 12-16 to 1.

    Priority Order (strict):
    1. ORDER retry queue (eligible items only)
    2. ORDER normal queue
    3. OTHER retry queue (eligible items only)
    4. OTHER normal queue

    Weight Distribution:
    - ORDER : OTHER = order_weight : other_weight (default 7:3)
    - retry : normal = retry_ratio : (1 - retry_ratio) within each group

    KEYS:
        KEYS[1] = queue:global:order         (ORDER normal queue - Sorted Set)
        KEYS[2] = queue:global:order:retry   (ORDER retry queue - Sorted Set)
        KEYS[3] = queue:global:other         (OTHER normal queue - Sorted Set)
        KEYS[4] = queue:global:other:retry   (OTHER retry queue - Sorted Set)
        KEYS[5] = leaky:global:bucket        (Rate limit bucket - Hash)

    ARGV:
        ARGV[1] = now                        (current timestamp in ms)
        ARGV[2] = total_slots                (max items to fetch)
        ARGV[3] = order_weight               (ORDER weight, e.g., 7)
        ARGV[4] = other_weight               (OTHER weight, e.g., 3)
        ARGV[5] = retry_ratio                (retry ratio within group, e.g., 0.7)
        ARGV[6] = retry_threshold            (retry eligibility threshold = now - 4000ms)
        ARGV[7] = leak_rate                  (current leak rate for bucket)
        ARGV[8] = capacity                   (bucket capacity)

    Returns:
        JSON string with polled items and statistics
        {
            "items": [
                {"queue": "order_retry", "data": "...", "score": 1234567890},
                ...
            ],
            "stats": {
                "order_retry": N,
                "order_normal": N,
                "other_retry": N,
                "other_normal": N,
                "total_polled": N,
                "remaining_slots": N
            },
            "bucket": {
                "water_level": N,
                "tokens_consumed": N
            }
        }

    Time Complexity: O(M log N) where M = total_slots, N = max queue size
--]]

-- Parse arguments
local orderNormalKey = KEYS[1]
local orderRetryKey = KEYS[2]
local otherNormalKey = KEYS[3]
local otherRetryKey = KEYS[4]
local bucketKey = KEYS[5]

local now = tonumber(ARGV[1])
local totalSlots = tonumber(ARGV[2])
local orderWeight = tonumber(ARGV[3])
local otherWeight = tonumber(ARGV[4])
local retryRatio = tonumber(ARGV[5])
local retryThreshold = tonumber(ARGV[6])
local leakRate = tonumber(ARGV[7])
local capacity = tonumber(ARGV[8])

-- Early return for zero or negative slots
if totalSlots <= 0 then
    return cjson.encode({
        items = {},
        stats = {
            order_retry = 0,
            order_normal = 0,
            other_retry = 0,
            other_normal = 0,
            total_polled = 0,
            remaining_slots = 0
        },
        bucket = {
            water_level = 0,
            tokens_consumed = 0
        }
    })
end

-- Initialize result structure
local result = {
    items = {},
    stats = {
        order_retry = 0,
        order_normal = 0,
        other_retry = 0,
        other_normal = 0,
        total_polled = 0,
        remaining_slots = totalSlots
    },
    bucket = {
        water_level = 0,
        tokens_consumed = 0
    }
}

-- Helper function: Poll items from a sorted set (ZRANGEBYSCORE + ZREM for retry, ZRANGE + ZREM for normal)
local function pollFromQueue(key, count, isRetry, threshold)
    if count <= 0 then
        return {}
    end

    local items
    if isRetry then
        -- For retry queues, only get items with score <= threshold (eligible for retry)
        items = redis.call('ZRANGEBYSCORE', key, '-inf', threshold, 'WITHSCORES', 'LIMIT', 0, count)
    else
        -- For normal queues, get items by index (FIFO order by score)
        items = redis.call('ZRANGE', key, 0, count - 1, 'WITHSCORES')
    end

    local polled = {}
    local toRemove = {}

    -- Items come as [member1, score1, member2, score2, ...]
    for i = 1, #items, 2 do
        local member = items[i]
        local score = items[i + 1]
        table.insert(polled, {data = member, score = tonumber(score)})
        table.insert(toRemove, member)
    end

    -- Atomically remove polled items
    if #toRemove > 0 then
        redis.call('ZREM', key, unpack(toRemove))
    end

    return polled
end

-- Helper function: Calculate slot allocation based on weights
local function calculateSlots(total, orderW, otherW, retryR)
    local totalWeight = orderW + otherW

    -- Calculate ORDER and OTHER base slots
    local orderTotal = math.floor(total * orderW / totalWeight + 0.5)  -- Round
    local otherTotal = total - orderTotal

    -- Calculate retry vs normal within each group
    local orderRetrySlots = math.floor(orderTotal * retryR + 0.5)
    local orderNormalSlots = orderTotal - orderRetrySlots

    local otherRetrySlots = math.floor(otherTotal * retryR + 0.5)
    local otherNormalSlots = otherTotal - otherRetrySlots

    return {
        order_retry = orderRetrySlots,
        order_normal = orderNormalSlots,
        other_retry = otherRetrySlots,
        other_normal = otherNormalSlots
    }
end

-- Helper function: Update leaky bucket (consume tokens)
local function updateBucket(tokensToConsume)
    -- Get current bucket state
    local waterLevel = tonumber(redis.call('HGET', bucketKey, 'water_level') or '0')
    local lastLeakTime = tonumber(redis.call('HGET', bucketKey, 'last_leak_time') or tostring(now))

    -- Calculate leaked amount
    local elapsedMs = now - lastLeakTime
    local elapsedSec = elapsedMs / 1000.0
    local leaked = leakRate * elapsedSec

    -- Apply leak
    waterLevel = math.max(0, waterLevel - leaked)

    -- Check available capacity
    local available = math.floor(capacity - waterLevel)
    local actualConsume = math.min(tokensToConsume, available)

    -- Update water level
    waterLevel = waterLevel + actualConsume

    -- Persist bucket state
    redis.call('HSET', bucketKey, 'water_level', tostring(waterLevel))
    redis.call('HSET', bucketKey, 'last_leak_time', tostring(now))
    redis.call('EXPIRE', bucketKey, 60)

    return actualConsume, waterLevel
end

-- Calculate initial slot allocation
local slots = calculateSlots(totalSlots, orderWeight, otherWeight, retryRatio)

-- Track remaining slots for redistribution
local remainingSlots = totalSlots
local polledItems = {}

-- Priority 1: ORDER retry queue (eligible items only)
local orderRetryItems = pollFromQueue(orderRetryKey, slots.order_retry, true, retryThreshold)
for _, item in ipairs(orderRetryItems) do
    item.queue = 'order_retry'
    table.insert(polledItems, item)
end
result.stats.order_retry = #orderRetryItems
remainingSlots = remainingSlots - #orderRetryItems

-- If ORDER retry didn't use all its slots, redistribute to ORDER normal
local orderRetryUnused = slots.order_retry - #orderRetryItems
local orderNormalSlots = slots.order_normal + orderRetryUnused

-- Priority 2: ORDER normal queue
local orderNormalItems = pollFromQueue(orderNormalKey, orderNormalSlots, false, 0)
for _, item in ipairs(orderNormalItems) do
    item.queue = 'order_normal'
    table.insert(polledItems, item)
end
result.stats.order_normal = #orderNormalItems
remainingSlots = remainingSlots - #orderNormalItems

-- Calculate unused ORDER slots to redistribute to OTHER
local orderTotalUsed = #orderRetryItems + #orderNormalItems
local orderTotalAllocated = slots.order_retry + slots.order_normal
local orderUnused = orderTotalAllocated - orderTotalUsed

-- Priority 3: OTHER retry queue (eligible items only)
local otherRetrySlots = slots.other_retry + math.floor(orderUnused * retryRatio + 0.5)
local otherRetryItems = pollFromQueue(otherRetryKey, otherRetrySlots, true, retryThreshold)
for _, item in ipairs(otherRetryItems) do
    item.queue = 'other_retry'
    table.insert(polledItems, item)
end
result.stats.other_retry = #otherRetryItems
remainingSlots = remainingSlots - #otherRetryItems

-- Redistribute unused OTHER retry slots to OTHER normal
local otherRetryUnused = otherRetrySlots - #otherRetryItems
local otherNormalSlots = slots.other_normal + otherRetryUnused + (orderUnused - math.floor(orderUnused * retryRatio + 0.5))

-- Priority 4: OTHER normal queue
local otherNormalItems = pollFromQueue(otherNormalKey, otherNormalSlots, false, 0)
for _, item in ipairs(otherNormalItems) do
    item.queue = 'other_normal'
    table.insert(polledItems, item)
end
result.stats.other_normal = #otherNormalItems
remainingSlots = remainingSlots - #otherNormalItems

-- Update statistics
result.stats.total_polled = #polledItems
result.stats.remaining_slots = remainingSlots

-- Copy items to result
result.items = polledItems

-- Update leaky bucket with consumed tokens
local tokensConsumed, newWaterLevel = updateBucket(#polledItems)
result.bucket.tokens_consumed = tokensConsumed
result.bucket.water_level = newWaterLevel

-- Return JSON result with error handling
local ok, encoded = pcall(cjson.encode, result)
if ok then
    return encoded
else
    -- Fallback: return minimal valid JSON on encoding error
    return '{"items":[],"stats":{"order_retry":0,"order_normal":0,"other_retry":0,"other_normal":0,"total_polled":0,"remaining_slots":0},"bucket":{"water_level":0,"tokens_consumed":0}}'
end
