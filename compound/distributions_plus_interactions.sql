-- total borrows and repayments per day and per asset
---- For further reading on both of these events see https://compound.finance/docs/ctokens#borrow and https://compound.finance/docs/ctokens#repay-borrow
WITH borrows_per_day AS (

        SELECT 
                --case when block_timestamp >= CURRENT_DATE - 30 then 'last_30'
                --     when block_timestamp >= CURRENT_DATE - 60 then '31_60'
                --     when block_timestamp >= CURRENT_DATE - 90 then '61_90'
                --     when block_timestamp >= CURRENT_DATE - 180 then '91_180'
                --     when block_timestamp >= CURRENT_DATE - 360 then '181_360'
                --     when block_timestamp < CURRENT_DATE - 360 then 'over_360'
                --     else '_none_' end as num_days_since,
                borrower as symbol_address, 
                borrows_contract_symbol AS underlying_symbol,
                    sum(loan_amount) AS token_loan_amount, 
                    sum(loan_amount_usd) AS loan_amount_usd,
                    count(distinct tx_id) as num_txs
        FROM compound.borrows 
        WHERE block_timestamp >= CURRENT_DATE - 30
        GROUP BY 1,2--,3        
    
), deposits_per_day AS (

        SELECT 
                --case when block_timestamp >= CURRENT_DATE - 30 then 'last_30'
                --     when block_timestamp >= CURRENT_DATE - 60 then '31_60'
                --     when block_timestamp >= CURRENT_DATE - 90 then '61_90'
                --     when block_timestamp >= CURRENT_DATE - 180 then '91_180'
                --     when block_timestamp >= CURRENT_DATE - 360 then '181_360'
                --     when block_timestamp < CURRENT_DATE - 360 then 'over_360'
                --     else '_none_' end as num_days_since,
                supplier as symbol_address,
                supplied_symbol AS underlying_symbol,
                    sum(supplied_base_asset) AS token_loan_amount, 
                    sum(supplied_base_asset_usd) AS loan_amount_usd,
                    count(distinct tx_id) as num_txs
        FROM compound.deposits
        WHERE block_timestamp >= CURRENT_DATE - 30
        GROUP BY 1,2--,3
    
), repays_per_day AS (

        SELECT 
                --case when block_timestamp >= CURRENT_DATE - 30 then 'last_30'
                --     when block_timestamp >= CURRENT_DATE - 60 then '31_60'
                --     when block_timestamp >= CURRENT_DATE - 90 then '61_90'
                --     when block_timestamp >= CURRENT_DATE - 180 then '91_180'
                --     when block_timestamp >= CURRENT_DATE - 360 then '181_360'
                --     when block_timestamp < CURRENT_DATE - 360 then 'over_360'
                --     else '_none_' end as num_days_since,
                borrower as borrower_address,
                --payer as payer_address,
                repay_contract_symbol AS underlying_symbol,
                    sum(repayed_amount) AS token_repay_amount, 
                    sum(repayed_amount_usd) AS loan_repay_amount_usd,
                    count(distinct tx_id) as num_txs
        FROM compound.repayments
        WHERE block_timestamp >= CURRENT_DATE - 30
        GROUP BY 1,2
    
), liquidations_per_day AS (

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
    
), redeems_per_day AS (

        SELECT 
                --case when block_timestamp >= CURRENT_DATE - 30 then 'last_30'
                --     when block_timestamp >= CURRENT_DATE - 60 then '31_60'
                --     when block_timestamp >= CURRENT_DATE - 90 then '61_90'
                --     when block_timestamp >= CURRENT_DATE - 180 then '91_180'
                --     when block_timestamp >= CURRENT_DATE - 360 then '181_360'
                --     when block_timestamp < CURRENT_DATE - 360 then 'over_360'
                --     else '_none_' end as num_days_since,
                supplier as supplier_address,
                --liquidator as liquidator_address,
                recieved_contract_symbol AS underlying_symbol,
                    sum(recieved_amount) AS token_redeem_amount, 
                    sum(recieved_amount_usd) AS loan_redeem_amount_usd,
                    count(distinct tx_id) as num_txs
        FROM compound.redemptions
        WHERE block_timestamp >= CURRENT_DATE - 30
        GROUP BY 1,2
    
)



select overall.*, whales.*
from
(select overall.*
, r.token_repay_amount
, r.loan_repay_amount_usd
, l.token_liquidation_amount
, l.loan_liquidation_amount_usd
, rd.token_redeem_amount
, rd.loan_redeem_amount_usd
, r.num_txs as num_repay
, l.num_txs as num_liquidations
, rd.num_txs as num_redemptions
, overall.borrow_amt_usd - r.loan_repay_amount_usd as net_borrowing
, overall.deposit_amt_usd - rd.loan_redeem_amount_usd as net_deposits

from
(SELECT 
        coalesce(b.symbol_address, d.symbol_address) as overall_address,
        coalesce(b.underlying_symbol, d.underlying_symbol) as underlying_symbol,
        --coalesce(b.num_days_since, r.num_days_since) as num_days_since,
        -- case when borrow_amt_token_order <= 20 then b.token_loan_amount else NULL end as borrow_amt_token,
        -- case when borrow_amt_token_order <= 20 then b.loan_amount_usd  else NULL end as borrow_amt_usd,
        -- case when deposit_amt_token_order <= 20 then d.token_loan_amount else NULL end as deposit_amt_token,
        -- case when deposit_amt_token_order <= 20 then d.loan_amount_usd  else NULL end as deposit_amt_usd
        b.token_loan_amount as borrow_amt_token,
        b.loan_amount_usd   as borrow_amt_usd,
        d.token_loan_amount  as deposit_amt_token,
        d.loan_amount_usd   as deposit_amt_usd,
        b.num_txs as num_borrows, d.num_txs as num_deposits
FROM 
    borrows_per_day b
    FULL OUTER JOIN
    deposits_per_day d
        ON b.symbol_address = d.symbol_address AND b.underlying_symbol = d.underlying_symbol --AND b.num_days_since = r.num_days_since
) overall 
    left OUTER JOIN
    repays_per_day r
        ON overall.overall_address = r.borrower_address AND overall.underlying_symbol = r.underlying_symbol
    left OUTER JOIN
    liquidations_per_day l
        ON overall.overall_address = l.borrower_address AND overall.underlying_symbol = l.underlying_symbol
    left OUTER JOIN
    redeems_per_day rd
        ON overall.overall_address = rd.supplier_address AND overall.underlying_symbol = rd.underlying_symbol 
) overall
INNER JOIN
    (select symbol_address, borrow_total_amt_usd, deposit_total_amt_usd,
        ROW_NUMBER() OVER ( ORDER BY borrow_total_amt_usd  DESC NULLS LAST) as borrow_amt_token_order,
        ROW_NUMBER() OVER ( ORDER BY deposit_total_amt_usd  DESC NULLS LAST) as deposit_amt_token_order
        FROM
        (select
        coalesce(b.symbol_address, d.symbol_address) as symbol_address,
        --sum(b.loan_amount_usd) OVER ( partition BY b.symbol_address ) as borrow_total_amt_usd,
        --sum(d.loan_amount_usd) OVER ( partition BY d.symbol_address ) as deposit_total_amt_usd
        sum(b.loan_amount_usd)  as borrow_total_amt_usd,
        sum(d.loan_amount_usd)  as deposit_total_amt_usd
        FROM 
        borrows_per_day b
        FULL OUTER JOIN
        deposits_per_day d
                ON b.symbol_address = d.symbol_address AND b.underlying_symbol = d.underlying_symbol
        group by 1)
        QUALIFY borrow_amt_token_order <= 20 or deposit_amt_token_order <= 20) whales
     on whales.symbol_address = overall.overall_address
