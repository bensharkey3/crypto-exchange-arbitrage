import json
import time
import requests
import hmac,hashlib
from collections import OrderedDict
import os


def lambda_handler(event, context):


    def ir_codelist():
        '''Returns all the ir codes
        '''
        ir_codes_url = 'https://api.independentreserve.com/Public/GetValidPrimaryCurrencyCodes'
        response = requests.get(ir_codes_url)
        response.raise_for_status()
        jsonResponse_codelist = response.json()
        
        return jsonResponse_codelist


    def ir_getopenorders(key, secret):
        '''Currently Open and Partially Filled orders. Returns list of OrderGuid's
        '''
        url='https://api.independentreserve.com/Private/GetOpenOrders'
        nonce=int(time.time()) 

        # make sure that parameter order is correct as specified in method documentation
        parameters = [ 
            url, 
            'apiKey=' + key,
            'nonce=' + str(nonce),
            'pageIndex=1', 
            'pageSize=100'
            ] 

        message = ','.join(parameters) 

        signature=hmac.new(
            secret.encode('utf-8'),
            msg=message.encode('utf-8'),
            digestmod=hashlib.sha256).hexdigest().upper() 

        # make sure this collection ordered in the same way as parameters
        data = OrderedDict([
            ("apiKey", key), 
            ("nonce", nonce), 
            ("signature", str(signature)), 
            ("pageIndex", 1), 
            ("pageSize", 100)])

        headers={'Content-Type': 'application/json'} 

        r = requests.post(url, data=json.dumps(data, sort_keys=False), headers=headers) 
        output = json.loads(r.content.decode('utf8'))
        
        open_orderguid_list = []
        for i in output['Data']:
            open_orderguid_list.append(i['OrderGuid'])
        
        return open_orderguid_list


    def ir_cancelorder(key, secret, orderguid):
        '''Cancels an outstanding order
        '''
        url = 'https://api.independentreserve.com/Private/CancelOrder'
        nonce=int(time.time()) 

        # make sure that parameter order is correct as specified in method documentation
        parameters = [ 
            url, 
            'apiKey=' + key,
            'nonce=' + str(nonce),
            'orderGuid=' + orderguid
            ] 

        message = ','.join(parameters) 

        signature=hmac.new(
            secret.encode('utf-8'),
            msg=message.encode('utf-8'),
            digestmod=hashlib.sha256).hexdigest().upper() 

        # make sure this collection ordered in the same way as parameters
        data = OrderedDict([
            ("apiKey", key), 
            ("nonce", nonce), 
            ("signature", str(signature)),
            ("orderGuid", orderguid)
            ])

        headers={'Content-Type': 'application/json'} 

        r = requests.post(url, data=json.dumps(data, sort_keys=False), headers=headers) 
        output = json.loads(r.content.decode('utf8'))
        
        return output


    def get_exch_rate():
        '''return AUD/USD exchange rate
        '''
        exch_rate = 'https://free.currconv.com/api/v7/convert?q=AUD_USD&compact=ultra&apiKey=60f48321df9478002ab8'.format(os.environ['currconv_key'])
        response = requests.get(exch_rate)
        response.raise_for_status()
        jsonResponse_exch_rate = response.json()
        jsonResponse_exch_rate['LambdaTimestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        lst_exch = []
        lst_exch.append(jsonResponse_exch_rate)

        return lst_exch


    def ir_getaccounts(key, secret):
        '''Returns my account balance in AUD
        '''
        url='https://api.independentreserve.com/Private/GetAccounts'
        nonce=int(time.time()) 

        # make sure that parameter order is correct as specified in method documentation
        parameters = [ 
            url, 
            'apiKey=' + key,
            'nonce=' + str(nonce)
            ] 

        message = ','.join(parameters) 

        signature=hmac.new(
            secret.encode('utf-8'),
            msg=message.encode('utf-8'),
            digestmod=hashlib.sha256).hexdigest().upper() 

        # make sure this collection ordered in the same way as parameters
        data = OrderedDict([
            ("apiKey", key),
            ("nonce", nonce), 
            ("signature", str(signature))
            ])

        headers={'Content-Type': 'application/json'} 

        r = requests.post(url, data=json.dumps(data, sort_keys=False), headers=headers) 
        output = json.loads(r.content.decode('utf8'))
        acct_bal_aud = output[0]['TotalBalance']
        return acct_bal_aud


    def main():
        '''Runs the crypto trading bot
        '''
        key='dba1cc8e-421f-44a7-ae97-6498be581022'
        secret='55ddc81bab0d4b12b60aca8f4d5c2220'

        # get a list of open orders
        open_orderguid_list = ir_getopenorders(key, secret)

        # cancel all open orders
        for i in open_orderguid_list:
            ir_cancelorder(key, secret, i)
        
        # hit binance api & xrates api to return binance buy and sell prices
        # multiply all prices by aud_usd rate, and clean
        lst_bn = get_binance_prices()
        for i in lst_bn:
            i['bidPrice'] = i['bidPrice']/AUD_USD
            i['askPrice'] = i['askPrice']/AUD_USD
            i['SecondaryCurrencyCode'] = 'AUD'
            i['symbol'] = i['symbol'].replace("TUSD", "")
            i['symbol'] = i['symbol'].replace("USDT", "")
            i['symbol'] = i['symbol'].replace("BUSD", "")
            i['symbol'] = i['symbol'].replace("USDC", "")


        # return exchange rate as float 
        lst_exch = get_exch_rate()
        AUD_USD = lst_exch[0]['AUD_USD']
        print('exchange rate is: '.format(AUD_USD))


        # git ir api to get price json, clean btc
        lst_ir = get_ir_prices()
        for i in lst_ir:
            i['symbol'] = i['symbol'].replace("XBT", "BTC")

        
        #compare ir and bn json lists to determine if buy opportunity > 1.025


    main()


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
