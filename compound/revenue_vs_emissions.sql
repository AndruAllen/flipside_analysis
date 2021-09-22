-- revenue for compound is reflected in the change in
-- reserve value for each market over the course of some time interval
-- in this case we use 1 day as the interval

-- first we pull the daily stats we need to calculate both reserve changes
-- and daily COMP emissions
with baseline as (
   select
    underlying_symbol,
    date_trunc('day',block_hour) as date,
    ctoken_address,
    avg(token_price) as underlying_token_price,
    avg(ctoken_price) as underlying_token_price_c,

    avg(reserves_token_amount) as reserves_ctoken,
    sum(comp_speed) * 2 as comp_emissions_comp, -- *2 because the contract reports the COMP emitted for EITHER the supply or borrow side of each market
    sum(comp_speed_usd) * 2 as comp_emissions_usd
   from compound.market_stats
    where block_hour > getdate() - interval '6 months'
    group by 1,2,3
),

-- now all we need is the daily difference in the raw reserves for each market
-- then we can simply sum up the diffs in the final step
reservediff as (
  select
    date,
    ctoken_address,
    reserves_ctoken,
    reserves_ctoken - lag(reserves_ctoken, 1) ignore nulls over (partition by ctoken_address order by date) as reserve_diff_ctoken,
    reserve_diff_ctoken * underlying_token_price as reserve_diff_usd
  from baseline
)

-- put it together: sum the diffs as revenue; sum the comp emissions (cost)
select 
   b.date, b.underlying_symbol,
   sum(rd.reserve_diff_usd) OVER(ORDER BY b.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as revenue_usd_30day_rolling_sum
   --sum(rd.reserve_diff_usd) as revenue_usd,
   --sum(b.comp_emissions_comp) as emissions_comp,
   --sum(b.comp_emissions_usd) as emissions_usd
   from baseline b join reservediff rd on b.date = rd.date 
   and b.ctoken_address = rd.ctoken_address
   --group by b.date, b.underlying_symbol
   where b.date > getdate() - interval '3 months'
   order by b.date desc;