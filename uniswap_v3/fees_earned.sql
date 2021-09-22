WITH collected_fees AS (
  SELECT
    block_timestamp,
    liquidity_provider,
    token0_symbol,
    token1_symbol,
    CASE WHEN amount0_usd IS NULL THEN 0 ELSE amount0_usd END AS amount0_usd,
    CASE WHEN amount1_usd IS NULL THEN 0 ELSE amount1_usd END AS amount1_usd
  FROM uniswapv3.position_collected_fees
),

token0s as (
    SELECT
      liquidity_provider,
      token0_symbol as token,
      -- p.price as pr,
      -- sum(amount0_adjusted) * pr as token_amount_earned
      SUM(amount0_usd) AS token_amount_earned
    FROM collected_fees
    
    -- JOIN ethereum.token_prices_hourly p
    --   ON f.token0_symbol = p.symbol
    --   AND date_trunc('hour',f.block_timestamp) = p.hour
    
    WHERE block_timestamp > '2021-05-05 12:00:00'
    GROUP BY 1,2
),

token1s as (
    SELECT
      liquidity_provider,
      token1_symbol as token,
      -- p.price as pr,
      -- sum(amount1_adjusted) * pr as token_amount_earned
      SUM(amount0_usd) AS token_amount_earned
    FROM collected_fees
    
    -- JOIN ethereum.token_prices_hourly p
    --   ON f.token1_symbol = p.symbol
    --   AND date_trunc('hour',f.block_timestamp) = p.hour
    
    WHERE block_timestamp > '2021-05-05 12:00:00'
    GROUP BY 1,2
),

tally as (
    SELECT * 
    FROM token0s 
    UNION 
    SELECT * 
    FROM token1s
)

SELECT
  liquidity_provider,
  token,
  sum(token_amount_earned) as fees_collected_usd
FROM tally
WHERE liquidity_provider <> LOWER('0xC36442b4a4522E871399CD717aBDD847Ab11FE88')
GROUP BY 1, 2
ORDER BY 3 DESC;


