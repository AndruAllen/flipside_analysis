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
                    sum(loan_amount_usd) AS loan_amount_usd
        FROM compound.borrows 
        WHERE block_timestamp >= CURRENT_DATE - 60
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
                    sum(supplied_base_asset_usd) AS loan_amount_usd
        FROM compound.deposits
        WHERE block_timestamp >= CURRENT_DATE - 60
        GROUP BY 1,2--,3
    
)

SELECT 
        coalesce(b.underlying_symbol, r.underlying_symbol) as underlying_symbol,
        coalesce(b.symbol_address, r.symbol_address) as symbol_address,
        --coalesce(b.num_days_since, r.num_days_since) as num_days_since,

        ROW_NUMBER() OVER ( ORDER BY b.loan_amount_usd  DESC NULLS LAST) as borrow_amt_token_order,
        ROW_NUMBER() OVER ( ORDER BY r.loan_amount_usd  DESC NULLS LAST) as deposit_amt_token_order,

        case when borrow_amt_token_order <= 50 then b.token_loan_amount else NULL end as borrow_amt_token,
        case when borrow_amt_token_order <= 50 then b.loan_amount_usd  else NULL end as borrow_amt_usd,

        case when deposit_amt_token_order <= 50 then r.token_loan_amount else NULL end as deposit_amt_token,
        case when deposit_amt_token_order <= 50 then r.loan_amount_usd  else NULL end as deposit_amt_usd

FROM 
    borrows_per_day b
    FULL OUTER JOIN
    deposits_per_day r
        ON b.symbol_address = r.symbol_address AND b.underlying_symbol = r.underlying_symbol --AND b.num_days_since = r.num_days_since
QUALIFY borrow_amt_token_order <= 50 or deposit_amt_token_order <= 50