--  Choose three of the largest liquidity providers. Are these providers concentrating their money in 
--   smaller ranges to risk getting higher returns or are they sticking to more of a V2 approach of 
--   a larger swap range? Has this changed over time? Has the price gone out of their range in the past 30 days?

WITH positions AS (
  SELECT * FROM uniswapv3.positions
),
   -- first get current positions:                         
max_blocks AS (
  SELECT
  min(block_id) AS min_block_id,
  max(block_id) AS max_block_id,
  pool_address,
  liquidity_provider,
  nf_token_id,
  date_trunc('day', block_timestamp) AS blk_date

  FROM positions
  
  GROUP BY
  pool_address,
  liquidity_provider,
  nf_token_id,
  blk_date
),
  max_blocks_prices AS (
  SELECT
  max(block_id) AS max_block_id,
  min(block_id) AS min_block_id,
  pool_address,
  date_trunc('day', block_timestamp) AS blk_date

  FROM uniswapv3.pool_stats
  
  GROUP BY
  pool_address,
  blk_date
),
current_positions_by_day AS (
  SELECT 
  p.pool_address,
  p.pool_name,
  p.nf_token_id,
  p.liquidity_provider,
  p.price_lower_1_0,
  p.price_upper_1_0,
  p.blk_date,
  case when p.block_id = mb.max_block_id then p.liquidity_adjusted else 0 end as max_liquidity_adjusted,
  case when p.block_id = mb_min.min_block_id then p.liquidity_adjusted else 0 end as min_liquidity_adjusted
    
  FROM (select *, date_trunc('day', block_timestamp) AS blk_date from positions) p
  left JOIN max_blocks mb ON 
  mb.blk_date = p.blk_date AND
  mb.max_block_id = p.block_id AND
  mb.pool_address = p.pool_address AND
  mb.liquidity_provider = p.liquidity_provider AND
  mb.nf_token_id = p.nf_token_id
  left JOIN max_blocks mb_min ON 
  mb_min.blk_date = p.blk_date AND
  mb_min.min_block_id = p.block_id AND
  mb_min.pool_address = p.pool_address AND
  mb_min.liquidity_provider = p.liquidity_provider AND
  mb_min.nf_token_id = p.nf_token_id
WHERE liquidity_adjusted > 0
  and (max_liquidity_adjusted > 0 or min_liquidity_adjusted > 0)
  -- and pool_name = 'USDC-WETH 3000 60'
), 
current_prices_by_day AS (
  SELECT 
  p.pool_address,
  p.pool_name,
  p.blk_date,
  case when p.block_id = mb.max_block_id then p.price_1_0 else 0 end as max_price_1_0,
  case when p.block_id = mb_min.min_block_id then p.price_1_0 else 0 end as min_price_1_0
    
  FROM (select *, date_trunc('day', block_timestamp) AS blk_date from uniswapv3.pool_stats) p
  left JOIN max_blocks_prices mb ON 
  mb.blk_date = p.blk_date AND
  mb.max_block_id = p.block_id AND
  mb.pool_address = p.pool_address
  left JOIN max_blocks_prices mb_min ON 
  mb_min.blk_date = p.blk_date AND
  mb_min.min_block_id = p.block_id AND
  mb_min.pool_address = p.pool_address
WHERE (max_price_1_0 > 0 or min_price_1_0 > 0)
  -- and pool_name = 'USDC-WETH 3000 60'
)
  
,  current_tvl as (
select tbl.*, row_number() over (order by tbl.tvl DESC) as rn
  from
(SELECT 
  pool_address, pool_name, --tick,
  --token0_symbol, token1_symbol,
  case when token0_symbol in ('USDC','USDT','DAI','FEI','EURS') and token1_symbol in ('USDC','USDT','DAI','FEI','EURS') then '_stable_stable_'
  		when token0_symbol in ('USDC','USDT','DAI','FEI','EURS') or token1_symbol in ('USDC','USDT','DAI','FEI','EURS') then '_stable_other_'
  		else '_other_other_' end as pairing_type
  ,date_trunc('day', block_timestamp) AS blk_date
    ,avg(token0_balance_usd + token1_balance_usd) as tvl
    --,sum(virtual_reserves_token0_usd + virtual_reserves_token1_usd) as tvl_plus_fees
  --,tvl_plus_fees - tvl as fees_maybe
  ,avg(virtual_liquidity_adjusted) as virtual_liquidity_adjusted
  ,avg(virtual_reserves_token0_adjusted+virtual_reserves_token1_adjusted) as vitrul_reserves_split
  --,sum(protocol_fees_token0_adjusted+protocol_fees_token1_adjusted) as protocol_fees_adjusted
  --,sum(virtual_reserves_token0_usd/virtual_reserves_token0_adjusted*protocol_fees_token0_adjusted + 
  --virtual_reserves_token1_usd/virtual_reserves_token1_adjusted*protocol_fees_token1_adjusted)  as protocol_fees_adj_usd
  --,sum(fee_growth_global0_x128*pow(2,-128)) as fee_growth_global0
  --,sum(fee_growth_global1_x128*pow(2,-128)) as fee_growth_global1
FROM uniswapv3.pool_stats 
WHERE 
blk_date = CURRENT_DATE() 
  --block_id in (select max(block_id) as block_id from max_blocks)
  and (token0_balance_usd + token1_balance_usd) is not NULL
  --and pool_address = '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8' 
  --and (token0_symbol in ('DAI') or token1_symbol in ('DAI'))
group by 1,2,3,4
order by tvl DESC) tbl
  --QUALIFY rn <= 5
  )

, price_range AS (
  SELECT 
  p.pool_address
  ,max(p.price_1_0) as  max_price_1_0_ever
  ,min(p.price_1_0) as  min_price_1_0_ever
  , abs(max_price_1_0_ever - min_price_1_0_ever) as diff
  FROM uniswapv3.pool_stats p
  group by 1
)
  
, tvl_by_day_lp_pool_band as (select pairing_type, pool.pool_name, pool.blk_date, pool.liquidity_provider, pool.price_lower_1_0, pool.price_upper_1_0
  ,case when price_lower_1_0 <= max_price_1_0 and price_upper_1_0 >= max_price_1_0 then '_in_range_'
  else '_out_of_range_' end as current_liquidity_state_max
  ,case when price_lower_1_0 <= min_price_1_0 and price_upper_1_0 >= min_price_1_0 then '_in_range_'
  else '_out_of_range_' end as current_liquidity_state_min

  
,avg(abs(pool.price_upper_1_0 - pool.price_lower_1_0)/ NULLIFZERO(rng.diff)) as avg_price_rng
,avg(max_price_1_0) as max_price_1_0
,avg(min_price_1_0) as min_price_1_0
,sum(zeroifnull(pool.max_liquidity_adjusted)) as max_liquidity_adjusted
,sum(zeroifnull(pool.min_liquidity_adjusted)) as min_liquidity_adjusted
,sum(zeroifnull(inc_dec.token_increases)) as token_increases
,sum(zeroifnull(inc_dec.token_decrease)) as token_decreases
--,count(distinct pool.liquidity_provider) as num_active_lps
-- price_lower_1_0

-- total_min_liquidity_adjusted
-- total_max_liquidity_adjusted

,sum(zeroifnull(pool.max_liquidity_adjusted)/NULLIFZERO(total_pool.total_max_liquidity_adjusted) * f.tvl) as max_curr_tvl_usd
,sum(zeroifnull(pool.min_liquidity_adjusted)/NULLIFZERO(total_pool.total_min_liquidity_adjusted) * f.tvl) as min_curr_tvl_usd

  
FROM current_positions_by_day pool
inner join current_tvl f
on pool.pool_address = f.pool_address
left join (select blk_date, pool_address, sum(min_liquidity_adjusted) as total_min_liquidity_adjusted, sum(max_liquidity_adjusted) as total_max_liquidity_adjusted
  			from current_positions_by_day group by 1,2) total_pool
on pool.pool_address = total_pool.pool_address
and f.blk_date = total_pool.blk_date
left JOIN current_prices_by_day price
on pool.pool_address = price.pool_address
and pool.blk_date = price.blk_date
left join (select pool_address, date_trunc('day', block_timestamp) AS blk_date, liquidity_provider, nf_token_id
  , sum(case when action = 'INCREASE_LIQUIDITY' then 1 else 0 end) as token_increases
  , sum(case when action = 'DECREASE_LIQUIDITY' then 1 else 0 end) as token_decrease
  from uniswapv3.lp_actions group by 1,2,3,4) inc_dec
on pool.pool_address = inc_dec.pool_address
and pool.blk_date = inc_dec.blk_date
and pool.liquidity_provider = inc_dec.liquidity_provider
and pool.nf_token_id = inc_dec.nf_token_id
left join (select * from price_range where diff > 0) as rng
on pool.pool_address = rng.pool_address
  -- where rng.diff 
group by 1,2,3,4,5,6,7,8)




, current_top_lps as (
  select tbl.liquidity_provider, avg_price_rng, tvl_weighted_avg_price_rng, curr_tvl_usd, row_number() over (order by curr_tvl_usd DESC) as rank
  from
  (select
  liquidity_provider
  -- , sum(max_curr_tvl_usd) as max_curr_tvl_usd
  , avg(avg_price_rng) as avg_price_rng
  , avg(avg_price_rng*min_curr_tvl_usd) as avg_price_rng_w
  , sum(min_curr_tvl_usd) as curr_tvl_usd
  , avg_price_rng_w / curr_tvl_usd as tvl_weighted_avg_price_rng
  
  from tvl_by_day_lp_pool_band
  where blk_date = CURRENT_DATE()
   and min_curr_tvl_usd > 0
  group by 1) tbl
    QUALIFY rank <= 3
  )


select * from current_top_lps