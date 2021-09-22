with big5 as (
select tbl.*, row_number() over (partition by pairing_type order by tbl.avg_tvl DESC) as Rank
  from
(SELECT 
  to_date(block_timestamp) as block_day, pool_name --tick,
  --token0_symbol, token1_symbol,
  ,case when token0_symbol in ('USDC','USDT','DAI','FEI','EURS') and token1_symbol in ('USDC','USDT','DAI','FEI','EURS') then '_Stable_Stable_'
  		when token0_symbol in ('USDC','USDT','DAI','FEI','EURS') or token1_symbol in ('USDC','USDT','DAI','FEI','EURS') then '_Stable_Unstable_'
  		else '_Unstable_Unstable_' end as pairing_type
    ,avg(token0_balance_usd + token1_balance_usd) as avg_tvl
    --,sum(virtual_reserves_token0_usd + virtual_reserves_token1_usd) as tvl_plus_fees
  --,tvl_plus_fees - tvl as fees_maybe
  -- ,avg(virtual_liquidity_adjusted) as virtual_liquidity_adjusted
  -- ,avg(virtual_reserves_token0_adjusted+virtual_reserves_token1_adjusted) as vitrul_reserves_split
  --,sum(protocol_fees_token0_adjusted+protocol_fees_token1_adjusted) as protocol_fees_adjusted
  --,sum(virtual_reserves_token0_usd/virtual_reserves_token0_adjusted*protocol_fees_token0_adjusted + 
  --virtual_reserves_token1_usd/virtual_reserves_token1_adjusted*protocol_fees_token1_adjusted)  as protocol_fees_adj_usd
  --,sum(fee_growth_global0_x128*pow(2,-128)) as fee_growth_global0
  --,sum(fee_growth_global1_x128*pow(2,-128)) as fee_growth_global1
FROM uniswapv3.pool_stats 
WHERE 
block_day = CURRENT_DATE() 
  --block_id in (select max(block_id) as block_id from max_blocks)
  and (token0_balance_usd + token1_balance_usd) is not NULL
  --and pool_address = '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8' 
  --and (token0_symbol in ('DAI') or token1_symbol in ('DAI'))
group by 1,2,3
order by avg_tvl DESC) tbl
  QUALIFY Rank <= 5)

select * from big5 order by pairing_type, rank