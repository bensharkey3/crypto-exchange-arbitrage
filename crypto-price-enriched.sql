SELECT 
    createdtimestamputc,
    lambdatimestamp,
    case when symbol = 'XBT' then 'BTC' else replace(replace(replace(REPLACE(symbol, 'TUSD', ''), 'BUSD'), 'USDT'), 'USDC') end as symbol_cleaned,
    case when exchange = 'binance' then max(bidprice)/max(r.aud_usd) else max(bidprice) end bidprice_aud,
    case when exchange = 'binance' then min(askprice)/max(r.aud_usd) else min(askprice) end askprice_aud,
    case when exchange = 'binance' then max(bidprice)/1.001/max(r.aud_usd) when exchange = 'independent reserve' then max(bidprice)/1.005 else null end bidprice_aud_incbrokerfee,
    case when exchange = 'binance' then min(askprice)*1.001/max(r.aud_usd) when exchange = 'independent reserve' then min(askprice)*1.005 else null end askprice_aud_incbrokerfee,
    exchange, 
    max(r.aud_usd) aud_usd
FROM "crypto-snapshot-db"."crypto_price_snapshots" s
left join "crypto-snapshot-db"."audusd_snapshots" r on r.lambdatimestamp = s.createdtimestamputc
where case when symbol = 'XBT' then 'BTC' else replace(replace(replace(REPLACE(symbol, 'TUSD', ''), 'BUSD'), 'USDT'), 'USDC') end != ''
and bidprice != 0
and askprice != 0
--and case when symbol = 'XBT' then 'BTC' else replace(replace(replace(REPLACE(symbol, 'TUSD', ''), 'BUSD'), 'USDT'), 'USDC') end = 'OMG'
group by 
    case when symbol = 'XBT' then 'BTC' else replace(replace(replace(REPLACE(symbol, 'TUSD', ''), 'BUSD'), 'USDT'), 'USDC') end,
    createdtimestamputc,
    lambdatimestamp,
    exchange
order by 
    case when symbol = 'XBT' then 'BTC' else replace(replace(replace(REPLACE(symbol, 'TUSD', ''), 'BUSD'), 'USDT'), 'USDC') end,
    createdtimestamputc desc,
    exchange desc
