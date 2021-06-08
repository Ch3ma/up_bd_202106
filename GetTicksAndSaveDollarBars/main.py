import pandas as pd
import requests as req
import json
import os
from datetime import datetime

from google.cloud import storage
from google.cloud import bigquery

api_key = 'ESTE NO SE COMPARTE'
PROJECT_ID = "chemackana-project"
BUCKET_NAME = "face_recog_bd"
REGION = "us-central1"
url = "https://api.polygon.io/v1/historic/crypto/"
FILE_WRITTEN = False
def getHistoryDataFrameCustom(url):
    tries = 10
    counter = 1
    while True:
        try:
            response = req.get(url)
            st = response.status_code
            break
        except Exception as e:
            print("Error with the connection, error {st}, retrying, {ct} of {tries}".format(st=st, ct=counter,
                                                                                                tries=tries))
            counter += 1
            if counter > tries:
                print("Error!, maximum number of tires")
                raise Exception("Error!, maximum number of tries")

    if st == 200:
        responseJSON = json.loads(response.content.decode('utf8').replace("'", '"'))
        ticks = pd.DataFrame(responseJSON['ticks'])
        if len(ticks) <= 0:
            print("The source responded, but the data is null, returning an empty frame")
            ticks = pd.DataFrame()
            offset = 0
        else:
            ticks = ticks[["t", "p", "s", "c"]]
            ticks["date"] = ticks["t"].apply(intToDate)
            offset = ticks.iloc[-1]["t"]
    else:
        print("Data not Found!, code {code} at url {url}".format(code = st, url = url))
        raise Exception("Data not Found!, code {code}".format(code = st, url = url))
    return ticks, st, offset

def intToDate(i):
    try:
        ts = int(i) / 1000
    except:
        print("Result is not an integer")
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def uploadToBucket(bucket_name, source_path, destination_path):
    try:
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(destination_path)
        blob.upload_from_filename(source_path)

    except Exception as e:
        print("Error {er}".format(str(e)))

def get_dollar_bars_fast(df, dollar_bar_limit=1000000):
    df["dollar_value"] = df["p"] * df["s"]
    df["bar_counter"] = df["dollar_value"].cumsum() // dollar_bar_limit
    dollar_bar_frame = df.groupby("bar_counter").agg(
        {"p": ["first", "max", "min", "last"], "t": ["last"], "s": ["sum"]})
    dollar_bar_frame.columns = ["open", "high", "low", "close", "date", "volume"]
    dollar_bar_frame["date"] = dollar_bar_frame["date"].apply(lambda x: intToDate(x))
    # check surplus registers
    surplus = df[df["bar_counter"] == df["bar_counter"].max()]
    sz = (surplus["p"] * surplus["s"]).sum()
    if sz < dollar_bar_limit:
        dollar_bar_frame.drop(dollar_bar_frame.tail(1).index, inplace=True)
    return dollar_bar_frame, surplus

def downloadSurplusFromBucket(table):
    try:
        client = storage.Client()
        bucket = client.get_bucket('bucket_1_cs')
        blob = bucket.blob("{table}_surplus.csv".format(table=table))
        print("downloading surplus")
        blob.download_to_filename("/tmp/{table}_surplus.csv".format(table=table))
        surplus = pd.read_csv("/tmp/{table}_surplus.csv".format(table=table), index_col = False)
        return surplus
    except Exception as e:
        print("Surplus not found, returning null with exception", str(e))
        return pd.DataFrame()

def getDayData(request):
    request_json = request.get_json(silent=True)
    request_args = request.args
    crypto = ""
    conversion = "USD"
    updateDate = ""
    process_status = "PENDING"
    dollar_value = 0

    if request_json and "crypto" in request_json:
        crypto = request_json["crypto"]
        updateDate = request_json["updateDate"]
        dollar_value = float(request_json["dollar_value"])
    elif request_args and "crypto" in request_args:
        crypto = request_args["crypto"]
        updateDate = request_args["updateDate"]
        dollar_value = float(request_args["dollar_value"])


    _url = url + crypto + "/" + conversion + "/" + updateDate + "?apiKey=" + api_key + "&limit=10000"
    _offset = ""
    _stCode = ""
    _retries = 10
    _error_counter = 0
    _dt = updateDate
    _writeHeader = True
    _outPutFullPath = f"/tmp/{crypto}_ticks_{_dt}.csv".format(crypto=crypto, datetime=updateDate)
    _firstLoop = True
    while True:

        if not _firstLoop:
            _url = "{url}{crypto}/{to}/{dt}?apiKey={apiKey}&limit=10000&offset={offset}".format(url=url, \
                                                                                                crypto=crypto, \
                                                                                                to=conversion, \
                                                                                                dt=_dt,
                                                                                                apiKey=api_key,
                                                                                                offset=_offset)
        else:
            _url = "{url}{crypto}/{to}/{dt}?apiKey={apiKey}&limit=10000".format(url=url, \
                                                                                crypto=crypto, \
                                                                                to=conversion, \
                                                                                dt=_dt,
                                                                                apiKey=api_key,
                                                                                )
        _firstLoop = False
        # print(_url)
        print(_url)
        _tmp_offset = _offset
        _tmp, _stCode, _offset = getHistoryDataFrameCustom(_url)


        if _offset == _tmp_offset:
            print("Offset repeated, finishing process")
            print("Veryfy that things got written")
            process_status = "OFFSET_REPEATED"
            uploadToBucket('ticks-cc', _outPutFullPath, "{crypto}_{dt}.csv".format(crypto = crypto, dt = updateDate))
            break

        # print(_offset)
        # print(_tmp.shape[0])
        if (_stCode == 200) and (_offset != 0) and _tmp.shape[0] > 1:
            # print("Here?")
            error_counter = 0
            if _writeHeader:
                _tmp.to_csv(_outPutFullPath, mode='a', header=True, index=False)
                _writeHeader = False
            else:
                _tmp.to_csv(_outPutFullPath, mode='a', header=False, index=False)
        else:  # Add a day
            if _offset == 0:
                print("Empty frame, ending process")
                process_status = "EMPTY_FRAME"
                uploadToBucket('ticks-cc', _outPutFullPath, "{crypto}_{dt}.csv".format(crypto = crypto, dt = updateDate))
                break
            elif _stCode != 200:
                print("Different from 200, current ", _stCode, " retrying ", error_counter, " from ", _retries)
                process_status = "ERROR IN CODE, RETURNING {st}".format(_stCode)
            elif _tmp.shape[0] == 1:
                _tmp.to_csv(_outPutFullPath, mode='a', header=False, index=False)
                print("Surplus register, writting and ending de process")
                process_status = "SURPLUS"
                uploadToBucket('ticks-cc', _outPutFullPath, "{crypto}_{dt}.csv".format(crypto = crypto, dt = updateDate))
                break

    if os.path.exists(_outPutFullPath):
        FILE_WRITTEN = True
        #get previous surplus
        tmp_surplus = downloadSurplusFromBucket(table = crypto)
        print("Calculating dollar bars")
        print("previous surplus ", tmp_surplus.shape[0])
        print("tmp shape", _tmp.shape)
        print("have to read?!")
        _tmp = pd.read_csv(_outPutFullPath)
        print("after read?!", _tmp.shape[0])
        print("concat shape", pd.concat([tmp_surplus, _tmp]).shape)
        dollar_bars, surplus = get_dollar_bars_fast(pd.concat([tmp_surplus, _tmp]), dollar_value)
        print("dollar bars shape ", dollar_bars.shape[0])
        print("surplus shape", surplus.shape[0])
        if dollar_bars.shape[0] > 0:
            bigquery_client = bigquery.Client()
            dollar_bar_table_id = "chemackana-project.ticks_test.{table}_DOLLAR_BARS".format(table=crypto.upper())
            print("Loading df into {table}".format(table=dollar_bar_table_id))
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=[
                    bigquery.SchemaField("open", "FLOAT64", mode="REQUIRED"),
                    bigquery.SchemaField("high", "FLOAT64", mode="REQUIRED"),
                    bigquery.SchemaField("low", "FLOAT64", mode="REQUIRED"),
                    bigquery.SchemaField("close", "FLOAT64", mode="REQUIRED"),
                    bigquery.SchemaField("date", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("volume", "FLOAT64", mode="REQUIRED"),
                ],
            )
            job = bigquery_client.load_table_from_dataframe(dollar_bars[["open", "high", "low", "close", "date", "volume"]],
                                                  job_config=job_config, destination=dollar_bar_table_id)

        bucket_surplus_path = "{crypto}_surplus.csv".format(crypto=crypto)
        local_surplus_path = "/tmp/" + bucket_surplus_path
        surplus[["t", "p", "s"]].to_csv(local_surplus_path, index=False)
        print("uploading surplus to bucket")
        uploadToBucket('bucket_1_cs', local_surplus_path, bucket_surplus_path)
        os.remove(_outPutFullPath)
        os.remove(local_surplus_path)

    else:
        raise Exception("File not written!")
    return {"process_status":process_status, "path":_outPutFullPath, "file_exists":FILE_WRITTEN, "dollar_bars_size":str(dollar_bars.shape[0]),\
            "t":str(surplus["t"].iloc[-1])}
