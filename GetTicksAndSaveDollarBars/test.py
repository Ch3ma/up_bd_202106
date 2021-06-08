import requests as req
import json as json

service_url = "https://us-central1-chemackana-project.cloudfunctions.net/GetTicksAndSaveDollarBars"
payload = {"dollar_value": "23088966", "crypto":"BTC", "updateDate":"2021-06-04", "sample_to_check":"1000000"}
#payload = {"dollar_value": "23088966", "crypto":"BTC", "updateDate":"2020-11-18", "sample_to_check":"1000000"}
response = req.get(service_url, params=payload)
#print(response.status_code)
#print(response.content)


"""
#create history
import pandas as pd
from datetime import datetime

#start = pd.to_datetime("2019-01-01").date()
#start = pd.to_datetime("2021-01-11").date()
#start = pd.to_datetime("2020-11-21").date()
#start = pd.to_datetime("2021-01-08").date()
start = pd.to_datetime("2019-01-01").date()
end = datetime.now().date()
retries = 10
error_counter = 0
#dollar_value = 23088966 #for BTC
#dollar_value = 40422834 #for ETH
#dollar_value = 4473073 #for ADA
#dollar_value = 12037226 #for LTC
dollar_value = 79428 #for BAT
for i in range(0, abs((start-end).days)):
    dt = start + pd.Timedelta("{n}D".format(n = i))
    #payload = {"crypto": "BTC", "updateDate": "{}".format(dt)}
    payload = {"dollar_value": str(dollar_value), "crypto": "BAT", "updateDate": "{}".format(dt)}
    while True:
        response = req.get(service_url, params=payload)
        if (response.status_code != 200) or ("ERROR" in json.loads(response.content)['process_status']):
            print("Error! {st}".format(st = response.status_code))
            error_counter += 1
            print("Retrying")
            if error_counter > retries:
                raise Exception("Dead at {}".format(dt))
        else:
            print("{} processed".format(dt))
            error_counter = 0
            print(response.content)
            break

"""