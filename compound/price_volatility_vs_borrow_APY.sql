-- In terms of borrowing, how often do APY rates change dramatically? 
-- Why might these changes take place? Show the sharpest changes in APY rates 
--   in the past six months and explain why these might have occurred.


with daily_stats as (SELECT
    -- block_hour,
  date_trunc('day',block_hour) as block_day,
    underlying_symbol,
    avg(supply_apy) as avg_supply_apy,
    stddev_samp(supply_apy) as std_dev_supply_apy,
    avg(comp_apy_supply) as avg_comp_apy_supply,

  	CORR( supply_apy, borrow_apy) as corr_supply_and_borrow_apys,
  	CORR( token_price, borrow_apy) as corr_price_and_borrow_apy,
  
    -- comp_apy_supply,
    avg_supply_apy+avg_comp_apy_supply as net_supply_apy,
  	avg(token_price) as avg_token_price,
  	stddev_samp(token_price) as std_dev_token_price,
 	std_dev_token_price / avg_token_price as vari_coef_token_price,
  
	avg(ctoken_price) as avg_ctoken_price,
  	stddev_samp(ctoken_price) as std_dev_ctoken_price,
 	std_dev_ctoken_price / avg_ctoken_price as vari_coef_ctoken_price,
  
    avg(borrow_apy) as avg_borrow_apy,
    STDDEV_SAMP(borrow_apy) as std_dev_borrow_apy,
 	std_dev_borrow_apy / avg_borrow_apy as vari_coef_avg_borrow_apy,
  
    avg(comp_apy_borrow) as avg_comp_apy_borrow,
    STDDEV_SAMP(comp_apy_borrow) as std_dev_comp_apy_borrow,
    avg_borrow_apy-avg_comp_apy_borrow as net_borrow_apy
FROM compound.market_stats
WHERE block_day >= to_date('2020-12-01')
    -- AND underlying_symbol in ('USDC','DAI','USDT','WBTC','ETH','UNI','COMP','ZRX','BAT')
   AND underlying_symbol <> 'SAI'
  and (block_hour != to_timestamp('2021-03-19T15:00:00Z') and contract_name != 'cWBTC2')
GROUP BY block_day, underlying_symbol
-- ORDER BY block_day, underlying_symbol
)
   

-- SELECT
-- -- date_trunc('day',block_hour) as block_day,
--     underlying_symbol, count(*) as cnt
-- FROM compound.market_stats
-- group by 1

select *  from daily_stats