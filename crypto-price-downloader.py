import json
import requests
import boto3
import datetime
import os


def lambda_handler(event, context):


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


    def get_ir_prices():
        '''returns current prices from independent reserve in a json object
        '''
        class CurrencyCode:
            def __init__(self, http):
                self.http = http
    
        ir_codes = 'https://api.independentreserve.com/Public/GetValidPrimaryCurrencyCodes'
                
        try:
            response = requests.get(ir_codes)
            response.raise_for_status()
            jsonResponse_codes = response.json()
        
            objs = []
            for i in jsonResponse_codes:
                objs.append(CurrencyCode('https://api.independentreserve.com/Public/GetMarketSummary?primaryCurrencyCode={}&secondaryCurrencyCode=aud'.format(i)))
                
            response_list = []
            for i in range(len(jsonResponse_codes)):
                response = requests.get(objs[i].http)
                jsonResponse = response.json()
                response_list.append(jsonResponse)

            # cleaning the json output
            lst_ir = []
            for i in response_list:
                del i['DayHighestPrice']
                del i['DayLowestPrice']
                del i['DayAvgPrice']
                del i['DayVolumeXbt']
                del i['DayVolumeXbtInSecondaryCurrrency']
                del i['LastPrice']
                i['symbol'] = i['PrimaryCurrencyCode']
                i['bidPrice'] = i['CurrentHighestBidPrice']
                i['askPrice'] = i['CurrentLowestOfferPrice']
                del i['CurrentLowestOfferPrice']
                del i['CurrentHighestBidPrice']
                del i['PrimaryCurrencyCode']
                i['exchange'] = 'independent reserve'
                i['CreatedTimestampUtc'] = i['CreatedTimestampUtc'].replace('T', ' ')
                i['CreatedTimestampUtc'] = i['CreatedTimestampUtc'].split('.', 1)[0]
                i['CreatedTimestampUtc'] = datetime.datetime.strptime(i['CreatedTimestampUtc'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')
                i['LambdaTimestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
                lst_ir.append(i)
    
        except Exception as err:
            lst_ir = '{}'.format(err)
            print(f'printing from function... error occurred: {err}')   

        return lst_ir 
    

    def get_binance_prices():
        '''returns current prices from binance in a json object
        '''
        timestamp = 'https://api.binance.com/api/v3/time'
        bookticker = 'https://api.binance.com/api/v3/ticker/bookTicker'
    
        try:
            # get binance timestamp
            response = requests.get(timestamp)
            response.raise_for_status()
            jsonResponse_timestamp = response.json()
            t = jsonResponse_timestamp['serverTime']
            time = datetime.datetime.utcfromtimestamp(float(t)/1000.)
        
            # get binance bookticket
            response = requests.get(bookticker)
            response.raise_for_status()
            jsonResponse_codes = response.json()
        
            # add timestamp to json and clean up
            lst_bn = []
            for i in jsonResponse_codes:
                i['CreatedTimestampUtc'] = time.strftime('%Y-%m-%d %H:%M')
                i['LambdaTimestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
                i['SecondaryCurrencyCode'] = 'USD'
                i['exchange'] = 'binance'
                i['bidPrice'] = float(i['bidPrice'])
                i['askPrice'] = float(i['askPrice'])
                del i['bidQty']
                del i['askQty']
                if i['symbol'].endswith('USD'):
                    lst_bn.append(i)

        except Exception as err:
            lst_bn = '{}'.format(err)
            print(f'printing from function... error occurred: {err}')   
        
        return lst_bn


    def write_to_s3(json_list, bucket_name):
        '''write json to file in s3
        '''
        try:
            object_name = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H-%M') + '.json'
            s3_object = boto3.resource('s3').Object(bucket_name, object_name)

            # convert json to required format (one object each line)
            json_str = json.dumps(json_list)
            json_str = json_str.replace('[', '')
            json_str = json_str.replace(']', '')
            json_str = json_str.replace('}, ', '}\n')
            json_bytes = json_str.encode('UTF-8')
            s3_object.put(Body=(bytes(json_bytes)))
    
        except Exception as err:
            print('printing from function... error writing to s3 bucket')

    
    def main():
        '''runs all the functions
        '''
        try:
            lst_ir = get_ir_prices()
            lst_bn = get_binance_prices()
            lst = lst_ir + lst_bn
            write_to_s3(lst, 'crypto-price-snapshots')

            lst_exch = get_exch_rate()            
            write_to_s3(lst_exch, 'audusd-snapshots')

        except Exception as e:
            print(e)
        

    main()

    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
