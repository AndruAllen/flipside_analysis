select date_trunc('day',pos.block_timestamp) as block_day,
--liquidity_provider,
sum(liquidity_adjusted),
count(distinct pos.liquidity_provider) as cnt

FROM uniswapv3.positions pos
inner join
(select liquidity_provider, min(block_timestamp) as min_block_timestamp from uniswapv3.positions group by 1) b
on pos.liquidity_provider = b.liquidity_provider and pos.block_timestamp = b.min_block_timestamp
  WHERE pos.pool_address in (SELECT distinct pool_address FROM uniswapv3.pools)
--  AND pos.block_timestamp > '2021-05-01 12:00:00'
--AND pos.block_timestamp < '2021-05-08 12:00:00'
--and pos.liquidity_provider = '0x84f1e9193810960766231a073396a20b4da88d9c'
and pos.is_active = True
and pos.liquidity_adjusted > 0
group by 1