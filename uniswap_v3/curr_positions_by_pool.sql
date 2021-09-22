WITH pools as (
  SELECT * FROM uniswapv3.pools
 )

 -- This query shows total liquidity being deposited and withdrawn from each individual Uniswap V3 Pools each day
, liquidity_pool AS (
    SELECT
        tx_id,
        date_trunc('day', block_timestamp) AS date,
        action,
        liquidity_provider,
        liquidity_adjusted,
        pool_address,
        pool_name,
        nf_token_id,
        price_lower_1_0,
        price_upper_1_0,
        price_lower_1_0_usd,
        price_upper_1_0_usd,
        token0_address,
        token1_address,
        token0_symbol,
        token1_symbol,
        amount0_adjusted,
        amount1_adjusted,
        amount0_usd,
        amount1_usd,
        (amount0_usd + amount1_usd) AS add_amount_total
    FROM uniswapv3.lp_actions
),
add_liquidity AS (
    SELECT
        date,
        liquidity_provider,
        pool_address,
        pool_name,
        --nf_token_id,
        sum(liquidity_adjusted) as liquidity_adjusted,
        SUM(add_amount_total) AS total_deposit
    FROM liquidity_pool 
    WHERE action = 'INCREASE_LIQUIDITY' 
    GROUP BY 1,2,3,4--,5
),
remove_liquidity AS (
    SELECT
        date,
        liquidity_provider,
        pool_address,
        pool_name,
        --nf_token_id,
        -sum(liquidity_adjusted) as liquidity_adjusted,
        -SUM(add_amount_total) AS total_withdrawl
    FROM liquidity_pool 
    WHERE action = 'DECREASE_LIQUIDITY' 
    GROUP BY 1,2,3,4--,5
),
days_to_fill as (
select lps.*, to_date(d.fake_day) as day_proj
from 
(select 
   row_number() over (order by null)  id, 
   --add_days('2021-05-01'::date, + id) - 1 
   dateadd(day, id-1, to_date('2021-05-01')) as fake_day
from table(generator(ROWCOUNT => 10000))
qualify fake_day <= to_date(current_date())) d
FULL OUTER JOIN (select liquidity_provider, pool_address, pool_name, min(date) as min_date from liquidity_pool group by 1,2,3) lps
on d.fake_day >= lps.min_date
)


-- /*
-- select date, sum(TOTAL_DEPOSITS_USD) as TOTAL_DEPOSITS_USD, sum(TOTAL_WITHDRAWLS_USD) as TOTAL_WITHDRAWLS_USD, count(distinct liquidity_provider) as num_liquidity_providers
-- from

, combo_sum as (SELECT
    coalesce(add_liquidity.date, remove_liquidity.date) AS date,
    coalesce(add_liquidity.liquidity_provider, remove_liquidity.liquidity_provider) AS liquidity_provider,
    coalesce(add_liquidity.pool_address, remove_liquidity.pool_address) AS pool_address,
    coalesce(add_liquidity.pool_name, remove_liquidity.pool_name) AS pool_name,
    --coalesce(add_liquidity.nf_token_id, remove_liquidity.nf_token_id) AS nf_token_id,
    total_deposit AS total_deposits_usd,
    total_withdrawl AS total_withdrawls_usd,
    zeroifnull(add_liquidity.liquidity_adjusted) + zeroifnull(remove_liquidity.liquidity_adjusted) as net_liquidity_adj

FROM add_liquidity
FULL OUTER JOIN remove_liquidity
  ON add_liquidity.pool_address = remove_liquidity.pool_address 
  AND add_liquidity.pool_name = remove_liquidity.pool_name 
  AND add_liquidity.date = remove_liquidity.date
  AND add_liquidity.liquidity_provider = remove_liquidity.liquidity_provider
  --AND add_liquidity.nf_token_id = remove_liquidity.nf_token_id

WHERE total_deposit is not NULL or total_withdrawl is not NULL
ORDER BY 1 DESC)

, lps_all_days as (select day_proj as block_timestamp, liquidity_provider, pool_address, pool_name
,sum(net_liquidity_adj) OVER(PARTITION BY liquidity_provider, pool_address, pool_name ORDER BY day_proj) as liquidity_adjusted
from
(select 
d.liquidity_provider, d.pool_address, d.pool_name, d.day_proj
, zeroifnull(s.net_liquidity_adj) as net_liquidity_adj
from days_to_fill d
full outer join combo_sum s
on d.liquidity_provider = s.liquidity_provider
and d.pool_address = s.pool_address
and d.pool_name = s.pool_name
and to_date(d.day_proj) = to_date(s.date)
--where d.liquidity_provider = '0x5acfbbf0aa370f232e341bc0b1a40e996c960e07'
--and d.pool_address = '0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801'
)
qualify liquidity_adjusted > 0)

, pos as (select * from (
  SELECT DISTINCT date_trunc('day',block_timestamp) as block_day,
      liquidity_provider,
      --nf_token_id,
      pool_address,
      pool_name,
      last_value(liquidity_adjusted) OVER(PARTITION BY block_day, liquidity_provider, pool_address, pool_name ORDER BY block_timestamp) as liquidity_adj

      -- last_value(tick_upper) OVER(PARTITION BY block_day, liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as tick_upper,
      -- last_value(tick_lower) OVER(PARTITION BY block_day, liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as tick_lower,

      -- last_value(tokens_owed0_usd) OVER(PARTITION BY block_day, liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as tokens_owed0_usd,
      -- last_value(tokens_owed1_usd) OVER(PARTITION BY block_day, liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as tokens_owed1_usd,

      -- last_value(price_lower_0_1_usd) OVER(PARTITION BY block_day, liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as price_lower_0_1_usd,
      -- last_value(price_upper_0_1_usd) OVER(PARTITION BY block_day, liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as price_upper_0_1_usd,

      -- last_value(price_lower_1_0_usd) OVER(PARTITION BY block_day, liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as price_lower_1_0_usd,
      -- last_value(price_upper_1_0_usd) OVER(PARTITION BY block_day, liquidity_provider, nf_token_id, pool_address, pool_name ORDER BY block_timestamp) as price_upper_1_0_usd
  FROM lps_all_days pos
  WHERE pos.pool_address in (SELECT pool_address
                             FROM pools)
  AND pos.block_timestamp > '2021-05-01 12:00:00'
  --and pos.is_active = True
) allpos
WHERE allpos.liquidity_adj > 0)

,hourly as (
SELECT
  DISTINCT date_trunc('hour',pstat.block_timestamp) as block_hour,
  pstat.pool_address,
  pool_name,
  
  last_value( VIRTUAL_RESERVES_TOKEN0_ADJUSTED * pow(VIRTUAL_RESERVES_TOKEN1_ADJUSTED,-1) ) OVER (PARTITION BY block_hour, pool_address, pool_name ORDER BY block_timestamp) as native_price0,
  last_value( VIRTUAL_RESERVES_TOKEN1_ADJUSTED * pow(VIRTUAL_RESERVES_TOKEN0_ADJUSTED,-1) ) OVER (PARTITION BY block_hour, pool_address, pool_name ORDER BY block_timestamp) as native_price1,
  
  last_value(tph0.price) OVER (PARTITION BY block_hour, pool_address, pool_name ORDER BY block_timestamp) as price0,
  last_value(tph1.price) OVER (PARTITION BY block_hour, pool_address, pool_name ORDER BY block_timestamp) as price1,
  
  last_value(token0_balance_adjusted) OVER (PARTITION BY block_hour, pool_address, pool_name ORDER BY block_timestamp) as gross_reserves_token0_adjusted,
  last_value(token1_balance_adjusted) OVER (PARTITION BY block_hour, pool_address, pool_name ORDER BY block_timestamp) as gross_reserves_token1_adjusted,
  
  last_value(token0_balance_usd) OVER (PARTITION BY block_hour, pool_address, pool_name ORDER BY block_timestamp) as token0_balance_usd,
  last_value(token1_balance_usd) OVER (PARTITION BY block_hour, pool_address, pool_name ORDER BY block_timestamp) as token1_balance_usd,

  last_value(virtual_liquidity_adjusted) OVER (PARTITION BY block_hour, pool_address, pool_name ORDER BY block_timestamp) as virtual_liquidity_adjusted,

  price0 * gross_reserves_token0_adjusted as token0_gross_usd,
  price1 * gross_reserves_token1_adjusted as token1_gross_usd,

  CASE WHEN price0 IS NULL and price1 IS NULL THEN 'no prices' 
       WHEN price0 IS NULL and price1 IS NOT NULL THEN 'price1' 
       WHEN price1 IS NULL and price0 IS NOT NULL THEN 'price0' 
       ELSE 'both prices' 
  END AS price_status
  FROM uniswapv3.pool_stats pstat
  
  LEFT JOIN ethereum.token_prices_hourly tph0 
    ON tph0.hour = date_trunc('hour',pstat.block_timestamp) 
    AND pstat.token0_address = tph0.token_address
  
  LEFT JOIN ethereum.token_prices_hourly tph1 
    ON tph1.hour = date_trunc('hour',pstat.block_timestamp) 
    AND pstat.token1_address = tph1.token_address
  
WHERE pstat.block_timestamp >= '2021-05-01 12:00:00'
ORDER BY block_hour DESC, pstat.pool_address
),

gussied as (
  SELECT
    block_hour, 
    pool_address,
    pool_name, 
    price_status,
    token0_balance_usd + token1_balance_usd as token_bal_usd,
    virtual_liquidity_adjusted,
    CASE 
      WHEN price_status = 'both prices' THEN token0_gross_usd + token1_gross_usd
      WHEN price_status = 'price1' THEN token1_gross_usd + ((gross_reserves_token0_adjusted * native_price1) * price1)
      WHEN price_status = 'price0' THEN token0_gross_usd + ((gross_reserves_token1_adjusted * native_price0) * price0)
      ELSE NULL 
     END AS tvl_usd
  FROM hourly
  WHERE price_status <> 'no prices'
),

daily_usd_tvl as (select block_day, pool_address, pool_name
, coalesce(tvl_usd,0) as tvl_usd
, coalesce(tvl_usd,0) / total_liquidity_adj as tvl_usd_per_liquidity
, coalesce(token_bal_usd, 0) as token_bal_usd
, coalesce(token_bal_usd,0) / total_liquidity_adj as token_bal_usd_per_liquidity

from 
(select pos.block_day, pos.pool_address, pos.pool_name, sum(liquidity_adj) as total_liquidity_adj
, avg(tvl_usd) as tvl_usd
, avg(token_bal_usd) as token_bal_usd

from pos
left join
(SELECT
  DISTINCT date_trunc('day',block_hour) as block_day,
  pool_address,
  --pool_name, 
  --token_bal_usd,
  --virtual_liquidity_adjusted,
  --tvl_usd as gross_liquidity
  last_value(token_bal_usd) OVER (PARTITION BY block_day, pool_address ORDER BY block_hour) as token_bal_usd,
  last_value(tvl_usd) OVER (PARTITION BY block_day, pool_address ORDER BY block_hour) as tvl_usd
FROM gussied 
WHERE tvl_usd <> 'NaN'
  --AND block_hour = (select max(block_hour) from gussied)
  ) price
on price.block_day = pos.block_day
and price.pool_address = pos.pool_address
group by 1,2,3))


--select p.block_day, p.liquidity_provider, p.pool_name, p.pool_address, sum(token_bal_usd_per_liquidity*liquidity_adj) as token_bal_usd

, top_30_may10 as (
select p.block_day, p.liquidity_provider, sum(token_bal_usd_per_liquidity*liquidity_adj) as token_bal_usd
from pos p
left join
daily_usd_tvl t
on p.block_day = t.block_day
and p.pool_address = t.pool_address
where token_bal_usd_per_liquidity > 0
and p.block_day = to_date('2021-05-10')
GROUP BY 1,2--,3,4
ORDER BY token_bal_usd DESC
limit 30)

, top_30_today as (
select p.block_day, p.liquidity_provider, sum(token_bal_usd_per_liquidity*liquidity_adj) as token_bal_usd
from pos p
left join
daily_usd_tvl t
on p.block_day = t.block_day
and p.pool_address = t.pool_address
where token_bal_usd_per_liquidity > 0
and p.block_day = to_date(CURRENT_DATE())
GROUP BY 1,2--,3,4
ORDER BY token_bal_usd DESC
limit 30)


select *, token_bal_usd / num_providers as avg_bal_by_lp
from
(select block_day
, case when top_liquidity_provider_may10 is not NULL and top_liquidity_provider_today is not NULL then 'Always_Top_30'
       when top_liquidity_provider_may10 is not NULL then 'Top_30_May10_Only'
       when top_liquidity_provider_today is not NULL then 'Top_30_Today_Only'
       else 'Not_Top_30' end as liquidity_provider_grp
, case when top_liquidity_provider_may10 is not NULL and top_liquidity_provider_today is not NULL then liquidity_provider else '_not_' end as top_6_address
, sum(token_bal_usd) as token_bal_usd
, count(distinct liquidity_provider) as num_providers

from 
(select p.block_day, p.liquidity_provider, sum(token_bal_usd_per_liquidity*liquidity_adj) as token_bal_usd
from pos p
left join
daily_usd_tvl t
on p.block_day = t.block_day
and p.pool_address = t.pool_address
where token_bal_usd_per_liquidity > 0
GROUP BY 1,2--,3,4
ORDER BY token_bal_usd DESC) a
left join (select distinct liquidity_provider as top_liquidity_provider_may10 from top_30_may10) b
on a.liquidity_provider = b.top_liquidity_provider_may10
left join (select distinct liquidity_provider as top_liquidity_provider_today from top_30_today) c
on a.liquidity_provider = c.top_liquidity_provider_today
group by 1,2,3)
