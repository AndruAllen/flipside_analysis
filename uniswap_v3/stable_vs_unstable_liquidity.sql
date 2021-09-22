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
  
,  big5 as (
select tbl.*, row_number() over (order by tbl.tvl DESC) as rn
  from
(SELECT 
  pool_address, pool_name, --tick,
  --token0_symbol, token1_symbol,
  case when token0_symbol in ('USDC','USDT','DAI','FEI','EURS') and token1_symbol in ('USDC','USDT','DAI','FEI','EURS') then '_stable_stable_'
  		when token0_symbol in ('USDC','USDT','DAI','FEI','EURS') or token1_symbol in ('USDC','USDT','DAI','FEI','EURS') then '_stable_other_'
  		else '_other_other_' end as pairing_type,
    to_date(block_timestamp) as block_day
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
block_day = CURRENT_DATE() 
  and pairing_type = '_stable_other_'
  --block_id in (select max(block_id) as block_id from max_blocks)
  and (token0_balance_usd + token1_balance_usd) is not NULL
  --and pool_address = '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8' 
  --and (token0_symbol in ('DAI') or token1_symbol in ('DAI'))
group by 1,2,3,4
order by tvl DESC) tbl
  QUALIFY rn <= 5)

,response as (select pool.pool_name, pool.blk_date, pool.liquidity_provider--, pool.price_lower_1_0, pool.price_upper_1_0
  ,case when price_lower_1_0 <= max_price_1_0 and price_upper_1_0 >= max_price_1_0 then '_in_range_'
  else '_out_of_range_' end as current_liquidity_state_max
  ,case when price_lower_1_0 <= min_price_1_0 and price_upper_1_0 >= min_price_1_0 then '_in_range_'
  else '_out_of_range_' end as current_liquidity_state_min
, avg(pool.price_lower_1_0) as avg_price_lower_1_0, avg(pool.price_upper_1_0) as avg_price_upper_1_0
,avg(max_price_1_0) as max_price_1_0
,avg(min_price_1_0) as min_price_1_0
,sum(zeroifnull(pool.max_liquidity_adjusted)) as max_liquidity_adjusted
,sum(zeroifnull(pool.min_liquidity_adjusted)) as min_liquidity_adjusted
,sum(zeroifnull(inc_dec.token_increases)) as token_increases
,sum(zeroifnull(inc_dec.token_decrease)) as token_decrease
--,count(distinct pool.liquidity_provider) as num_active_lps
-- price_lower_1_0
  
FROM current_positions_by_day pool
inner join big5 F
on pool.pool_address = f.pool_address
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
  -- where
group by 1,2,3,4,5)



, movement as (select blk_date, liquidity_provider, pool_name
  , current_liquidity_state_min
  , case when token_decrease > 0 and token_increases > 0 and current_liquidity_state_min = '_out_of_range_' and current_liquidity_state_max = '_in_range_' 
  			and min_liquidity_adjusted > 0 and max_liquidity_adjusted > 0 then 'Updated_range'
         when current_liquidity_state_min = '_out_of_range_' and current_liquidity_state_max = '_in_range_' 
  			and min_liquidity_adjusted > 0 and max_liquidity_adjusted > 0 then 'Floated_into_range'
         when current_liquidity_state_min = '_out_of_range_' and current_liquidity_state_max = '_out_of_range_' 
  			and min_liquidity_adjusted > 0 and max_liquidity_adjusted > 0 then 'Still_out_of_range'
  		when current_liquidity_state_min = '_out_of_range_' and current_liquidity_state_max = '_out_of_range_' 
  			and min_liquidity_adjusted > 0 and token_decrease > 0 then 'Reduced_exposure'
  		 else 'Other' end as change_that_day
  , sum(min_liquidity_adjusted) as min_liquidity_adjusted
  , sum(max_liquidity_adjusted) as max_liquidity_adjusted
  -- , case when token_decrease > 0 then 1 else 0 end as token_decrease_flg
  -- , case when token_increases > 0 then 1 else 0 end as token_increase_flg
  -- , current_liquidity_state_min, current_liquidity_state_max
  , avg(min_price_1_0) as min_price_1_0
  , avg(max_price_1_0) as max_price_1_0
  from  response
group by 1,2,3,4,5)


 , per_pool as (select a.blk_date, a.pool_name, a.change_that_day
,a.num_active_lps / b.num_active_lps as pct_num_active_lps
,a.min_liquidity_adjusted / b.min_liquidity_adjusted as pct_min_liquidity_adjusted
-- ,a.max_liquidity_adjusted / b.max_liquidity_adjusted as pct_max_liquidity_adjusted
,a.min_price_1_0 as st_price_1_0
,a.max_price_1_0 as end_price_1_0
from
(select mv.blk_date, mv.pool_name, mv.change_that_day
  ,count(DISTINCT mv.liquidity_provider) as num_active_lps
  ,sum(mv.min_liquidity_adjusted) as min_liquidity_adjusted
  ,sum(mv.max_liquidity_adjusted) as max_liquidity_adjusted
  , avg(mv.min_price_1_0) as min_price_1_0
  , avg(mv.max_price_1_0) as max_price_1_0
  
  from movement as mv
where current_liquidity_state_min = '_out_of_range_'
group by 1,2,3) a
left join (select mv.blk_date, mv.pool_name
  ,count(DISTINCT mv.liquidity_provider) as num_active_lps
  ,sum(mv.min_liquidity_adjusted) as min_liquidity_adjusted
  ,sum(mv.max_liquidity_adjusted) as max_liquidity_adjusted
  
  from movement as mv
where current_liquidity_state_min = '_out_of_range_'
group by 1,2) b
on a.blk_date = b.blk_date
and a.pool_name = b.pool_name
  where b.num_active_lps > 0 and b.min_liquidity_adjusted > 0
  )


select a.blk_date, a.change_that_day
, a.pct_num_active_lps_avg / total_pct_num_active_lps_avg as pct_num_active_lps_avg
, a.pct_min_liquidity_adjusted_avg / total_pct_min_liquidity_adjusted_avg as pct_min_liquidity_adjusted_avg
from
(select blk_date, change_that_day
, avg(pct_num_active_lps) as pct_num_active_lps_avg
, avg(pct_min_liquidity_adjusted) as pct_min_liquidity_adjusted_avg
-- , avg(pct_max_liquidity_adjusted) as pct_max_liquidity_adjusted_avg
from per_pool
-- where pool_name = 'FEI-USDC 500 10'
group by 1,2) a
left join
(select blk_date
, sum(pct_num_active_lps_avg) as total_pct_num_active_lps_avg
, sum(pct_min_liquidity_adjusted_avg) as total_pct_min_liquidity_adjusted_avg
from
  (select blk_date, change_that_day
, avg(pct_num_active_lps) as pct_num_active_lps_avg
, avg(pct_min_liquidity_adjusted) as pct_min_liquidity_adjusted_avg
-- , avg(pct_max_liquidity_adjusted) as pct_max_liquidity_adjusted_avg
from per_pool
-- where pool_name = 'FEI-USDC 500 10'
group by 1,2)
group by 1) b
on a.blk_date = b.blk_date

-- select * from movement

--   ,sum(case when current_liquidity_state = '_in_range_' then sum_liquidity_adjusted else 0 end) as liquidity_adjusted_in_range
--   ,sum(case when current_liquidity_state = '_out_of_range_' then sum_liquidity_adjusted else 0 end) as liquidity_adjusted_out_of_range
--   ,sum(case when current_liquidity_state = '_in_range_' then num_active_lps else 0 end) as num_active_lps_in_range
--   ,sum(case when current_liquidity_state = '_out_of_range_' then num_active_lps else 0 end) as num_active_lps_out_of_range
--   ,sum(sum_liquidity_adjusted) as total_liquidity_adjusted
--   ,sum(num_active_lps) as total_num_active_lps
  
-- ,liquidity_adjusted_in_range/total_liquidity_adjusted as pct_liquidity_in_range
-- ,liquidity_adjusted_out_of_range/total_liquidity_adjusted as pct_liquidity_out_of_range
  
-- ,num_active_lps_in_range/total_num_active_lps as pct_active_lps_in_range
-- ,num_active_lps_out_of_range/total_num_active_lps as pct_active_lps_out_of_range

