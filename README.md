# crypto-exchange-arbitrage
Use AWS Lambda to hit crypto exchange API's and save buy and sell order prices every 15mins to s3. Then use AWS Athena and Quicksight to query files in s3 to see if there are any arbitrage opportunities. If arbitrage opportunities are evident, i'll investigate creating an automated trading bot.
