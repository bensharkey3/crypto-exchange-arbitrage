select 
    ir.lambdatimestamp,
    ir.symbol_cleaned,
    max(ir.bidprice_aud_incbrokerfee) ir_bidprice_aud_incbrokerfee,
    min(ir.askprice_aud_incbrokerfee) ir_askprice_aud_incbrokerfee,
    max(bi.bi_bidprice_aud_incbrokerfee) bi_bidprice_aud_incbrokerfee,
    min(bi.bi_askprice_aud_incbrokerfee) bi_askprice_aud_incbrokerfee,
    max(bi.bi_bidprice_aud_incbrokerfee)/min(ir.askprice_aud_incbrokerfee) buy_ir_arbitrage,
    min(ir.bidprice_aud_incbrokerfee)/max(bi.bi_askprice_aud_incbrokerfee) sell_ir_arbitrage
from "crypto-snapshot-db"."crypto_price_enriched" ir
left join (
    SELECT 
        lambdatimestamp,
        symbol_cleaned,
        max(bidprice_aud_incbrokerfee) bi_bidprice_aud_incbrokerfee,
        min(askprice_aud_incbrokerfee) bi_askprice_aud_incbrokerfee

    FROM "crypto-snapshot-db"."crypto_price_enriched" 
    where exchange = 'binance'
    group by 
        lambdatimestamp,
        symbol_cleaned
) bi on bi.lambdatimestamp = ir.lambdatimestamp and bi.symbol_cleaned = ir.symbol_cleaned
where ir.exchange = 'independent reserve'
group by 
    ir.lambdatimestamp,
    ir.symbol_cleaned
order by 
    ir.symbol_cleaned,
    ir.lambdatimestamp desc
