WITH borrows_per_day AS (

        SELECT 
                --case when block_timestamp >= CURRENT_DATE - 30 then 'last_30'
                --     when block_timestamp >= CURRENT_DATE - 60 then '31_60'
                --     when block_timestamp >= CURRENT_DATE - 90 then '61_90'
                --     when block_timestamp >= CURRENT_DATE - 180 then '91_180'
                --     when block_timestamp >= CURRENT_DATE - 360 then '181_360'
                --     when block_timestamp < CURRENT_DATE - 360 then 'over_360'
                --     else '_none_' end as num_days_since,
                --borrower as wallet_address,
                date_trunc('day',block_timestamp) as block_day,
                borrows_contract_symbol AS underlying_symbol,
                    sum(loan_amount) AS token_loan_amount, 
                    sum(loan_amount_usd) AS loan_amount_usd,
                    count(distinct borrower) as num_users,
                    count(distinct tx_id) as dist_num_txs,
                    count(tx_id) as num_txs
        FROM compound.borrows 
        WHERE block_timestamp >= CURRENT_DATE - 180
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
                --supplier as wallet_address,
                date_trunc('day',block_timestamp) as block_day,
                supplied_symbol AS underlying_symbol,
                    sum(supplied_base_asset) AS token_loan_amount, 
                    sum(supplied_base_asset_usd) AS loan_amount_usd,
                    count(distinct supplier) as num_users,
                    count(distinct tx_id) as dist_num_txs,
                    count(tx_id) as num_txs
        FROM compound.deposits
        WHERE block_timestamp >= CURRENT_DATE - 180
        GROUP BY 1,2--,3
    
)

select *
, borrows_usd_30day_rolling_sum/(deposits_usd_30day_rolling_sum) as borrowing_as_pct_of_deposits_usd_30day_rolling
, dist_num_borrows_30day_rolling_sum/(dist_num_deposits_30day_rolling_sum) as dist_num_of_borrows_as_pct_of_deposits_30day_rolling
, num_borrowers_30day_rolling_avg/(num_depositors_30day_rolling_avg) as avg_num_borrowers_as_pct_of_depositors_30day_rolling
, -1*borrows_usd_30day_rolling_sum as neg_borrows_usd_30day_rolling_sum
, -1*dist_num_borrows_30day_rolling_sum as neg_dist_num_borrows_30day_rolling_sum
, -1*num_borrowers_30day_rolling_avg as neg_num_borrowers_30day_rolling_avg
, deposits_usd_30day_rolling_sum / dist_num_deposits_30day_rolling_sum as avg_deposit_size_usd_30day_rolling
, borrows_usd_30day_rolling_sum / dist_num_borrows_30day_rolling_sum as avg_borrow_size_usd_30day_rolling



from
(select 
   coalesce(b.block_day,d.block_day) overall_block_day, coalesce(b.underlying_symbol,d.underlying_symbol) as underlying_symbol
   ,sum(b.loan_amount_usd) OVER(PARTITION BY b.underlying_symbol ORDER BY b.block_day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as borrows_usd_30day_rolling_sum
   ,sum(d.loan_amount_usd) OVER(PARTITION BY d.underlying_symbol ORDER BY d.block_day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as deposits_usd_30day_rolling_sum
   ,sum(b.dist_num_txs) OVER(PARTITION BY b.underlying_symbol ORDER BY b.block_day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as dist_num_borrows_30day_rolling_sum
   ,sum(d.dist_num_txs) OVER(PARTITION BY d.underlying_symbol ORDER BY d.block_day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as dist_num_deposits_30day_rolling_sum
   ,sum(b.num_txs) OVER(PARTITION BY b.underlying_symbol ORDER BY b.block_day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as num_borrows_30day_rolling_sum
   ,sum(d.num_txs) OVER(PARTITION BY d.underlying_symbol ORDER BY d.block_day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as num_deposits_30day_rolling_sum
   ,avg(b.num_users) OVER(PARTITION BY b.underlying_symbol ORDER BY b.block_day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as num_borrowers_30day_rolling_avg
   ,avg(d.num_users) OVER(PARTITION BY d.underlying_symbol ORDER BY d.block_day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as num_depositors_30day_rolling_avg

   --sum(rd.reserve_diff_usd) as revenue_usd,
   --sum(b.comp_emissions_comp) as emissions_comp,
   --sum(b.comp_emissions_usd) as emissions_usd
   from borrows_per_day b
   full outer join
   deposits_per_day d
   on b.block_day = d.block_day and b.underlying_symbol = d.underlying_symbol
   --group by b.date, b.underlying_symbol
   --where b.date > getdate() - interval '3 months'
   order by overall_block_day desc)