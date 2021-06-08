import requests as req
import json as json

service_url = "https://us-central1-chemackana-project.cloudfunctions.net/GetHRP"
payload = {"days":"30" , "crypto":"LTC-ETH-BTC-BAT", "end_date": "2019-06-01"}
response = req.get(service_url, params=payload)
print(response.status_code)
print(response.content)