with liquidations_per_day AS (

        SELECT 
                --case when block_timestamp >= CURRENT_DATE - 30 then 'last_30'
                --     when block_timestamp >= CURRENT_DATE - 60 then '31_60'
                --     when block_timestamp >= CURRENT_DATE - 90 then '61_90'
                --     when block_timestamp >= CURRENT_DATE - 180 then '91_180'
                --     when block_timestamp >= CURRENT_DATE - 360 then '181_360'
                --     when block_timestamp < CURRENT_DATE - 360 then 'over_360'
                --     else '_none_' end as num_days_since,
                borrower as borrower_address,
                --liquidator as liquidator_address,
                liquidation_contract_symbol AS underlying_symbol,
                    sum(liquidation_amount) AS token_liquidation_amount, 
                    sum(liquidation_amount_usd) AS loan_liquidation_amount_usd,
                    count(distinct tx_id) as num_txs
        FROM compound.liquidations
        WHERE block_timestamp >= CURRENT_DATE - 30
        GROUP BY 1,2
    
)

select * from (select underlying_symbol, block_hour
, sum(token_price) as token_price, avg(token_price) as avg_token_price
--, sum(ctoken_price) as ctoken_price, avg(ctoken_price) as avg_ctoken_price
from compound.market_stats
WHERE underlying_symbol in ('USDT','DAI','USDC')
group by 1,2)
where avg_token_price > 1.01 or avg_token_price < .99
