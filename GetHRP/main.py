import pandas as pd
import numpy as np
import requests as req
import json
from datetime import datetime
from sklearn import cluster as cl
from flask import abort
from google.cloud import storage

API_KEY = 'ESTA NO SE COMPARTE'

def getDailyBars(ticker, start, end):
    base_url = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?unadjusted=false&sort=asc&limit=2000&apiKey=nSC6CkHwJJQV93FofPmBVWDDUCjU5zXB"
    url = base_url.format(ticker=ticker, start=start, end=end)
    df = pd.DataFrame()
    response = req.get(url)
    print("this url ", url)
    df = pd.concat([pd.DataFrame(json.loads(response.content)['results'])], axis = 1)
    df["date"] = df["t"].apply(lambda x: intToDate(x))
    df.set_index("date", inplace = True)
    return df

def intToDate(i):
    try:
        ts = int(i) / 1000
    except:
        print("Result is not an integer")
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def correlDist(corr):
    # A distance matrix based on correlation, where 0<=d[i,j]<=1
    # This is a proper distance metric
    dist=((1-corr)/2.)**.5 # distance matrix
    return dist

def setIndexes(u):
    l = []
    for i in u:
        l.append(list(list(np.where(u == i))[0]))
    l.sort()
    last = l[-1]
    for i in range(len(l) - 2, -1, -1):
        if last == l[i]:
            del l[i]
        else:
            last = l[i]
    return l

def getIVP(cov,**kargs):
    # Compute the inverse-variance portfolio
    ivp=1./np.diag(cov)
    ivp/=ivp.sum()
    x = pd.Series(ivp)
    x.index = cov.columns[x.index]
    #return pd.Series(ivp)
    return x

def getClusterVar(cov,cItems):
    # Compute variance per cluster
    cov_=cov.loc[cItems,cItems] # matrix slice
    w_=getIVP(cov_).values.reshape(-1,1)
    cVar=np.dot(np.dot(w_.T,cov_),w_)[0,0]
    return cVar

def getSplitSpectral(cova, sortIx):
    #uIx = np.unique(reduce(operator.add, sortIx))
    uIx = []
    for i in sortIx:
        if len(i) > 1:
            for j in i:
                uIx.append(j)
        else:
            uIx.append(i[0])
    w = pd.Series(1.0, index=uIx)
    cItems = sortIx
    i = 0
    #labels
    labels = cova.columns[uIx]
    #><
    while len(cItems) > 0:
        if len(cItems[0]) > 1:
            #first, compute the array with the rest.
            # like [A, B, C, D][E, F, G]
            nvocItems = cItems[0]
            nvocItems2 = list(set(uIx) - set(cItems[0]))
            if not (not nvocItems2):
                cVar0 = getClusterVar(cova, labels[nvocItems])
                cVar1 = getClusterVar(cova, labels[nvocItems2])
                alpha = 1 - cVar0 / (cVar0 + cVar1)
                #w[nvocItems[0]] *= alpha
                w[nvocItems] *= alpha
                w[nvocItems2] *= 1 - alpha
                #then, compute the array within self
                #like [A][B, C, D], [B][C, D], [C][D]
            for j in range(0, len(cItems[0]) - 1):
                nvocItems = [cItems[0][j]]
                nvocItems2 = cItems[0][1+j:]
                cVar0 = getClusterVar(cova, labels[nvocItems])
                cVar1 = getClusterVar(cova, labels[nvocItems2])
                alpha = 1 - cVar0 / (cVar0 + cVar1)
                w[nvocItems[0]] *= alpha
                w[nvocItems2] *= 1 - alpha

            #if its the last iteration and the last element
            #of the array is of the form [A, B, C, D], compute only [A][B, C, D], [B][C, D], [C][D]

        else:
            #At this case, the first part of the chain is of length 1
            nvocItems = [uIx[0]]

            nvocItems2 = uIx[1:]
            #if the second part of the chain is of length 1, convert it to array
            #if len(uIx[1:]) == 1:
            #    nvocItems2 = [uIx[1:]]

            cVar0 = getClusterVar(cova, labels[nvocItems])
            # if the second part of the chain is of length 1, convert it to array
            cVar1 = getClusterVar(cova, labels[nvocItems2])
            alpha = 1 - cVar0 / (cVar0 + cVar1)
            w[nvocItems[0]] *= alpha
            w[nvocItems2] *= 1 - alpha

        cItems.pop(0)
        if cItems:
            #uIx = np.unique(reduce(operator.add, sortIx))
            uIx = []
            for i in cItems:
                if len(i) > 1:
                    for j in i:
                        uIx.append(j)
                else:
                    uIx.append(i[0])
        #If only 1 element of length 1 remains in cItems, this is, uIx is of len(uIx) = 1
        #then break
        if len(uIx) <= 1:
            break
    w.index = labels[w.index]
    return w

def getHRPSpectralWeights(df, n_clusters = None):
    cov, corr = df.cov(), df.corr()
    dist = correlDist(corr)
    if n_clusters is None:
        s = cl.SpectralClustering(random_state=2**31-1)
    else:
        s = cl.SpectralClustering(n_clusters=n_clusters, random_state=2**31-1)
    u = s.fit_predict(dist)
    l = []
    l = setIndexes(u)
    hrpSpec = getSplitSpectral(cov, l)
    return hrpSpec

def uploadToBucket(bucket_name, source_path, destination_path):
    try:
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(destination_path)
        blob.upload_from_filename(source_path)

    except Exception as e:
        print("Error {er}".format(str(e)))

#cryptos = ADA-BTC-MANA,....
#this method is not sensitive to dates, it takes the full data set to train
def getHRPWeights(request):
    request_json = request.get_json(silent=True)
    request_args = request.args
    cryptos = None
    start_date = None
    end_date = None
    days = 0
    if request_json and "crypto" in request_json:
        cryptos = request_json["crypto"].split('-')
        end_date = request_json["end_date"]
        days = int(request_json["days"])
    elif request_args and "crypto" in request_args:
        cryptos = request_args["crypto"].split('-')
        end_date = request_args["end_date"]
        days = int(request_args["days"])

    if len(cryptos) <= 2:
        return abort(400, "Not enough stocks")
    elif days <= 3:
        return abort(400, "Not enough days")

    prices = pd.DataFrame()
    start_date = (pd.Timestamp(end_date) - pd.Timedelta("{}D".format(days))).strftime("%Y-%m-%d")
    print("From {} to {}".format(start_date, end_date))
    for i in cryptos:
        tmp = getDailyBars("X:{}USD".format(i), start_date, end_date)[["c"]]
        tmp.columns = ["{}_close".format(i)]
        prices = pd.concat([prices, tmp], axis = 1)
    prices.columns = cryptos
    prices = prices.pct_change(1).dropna()
    print("Getting HRP")
    weights = getHRPSpectralWeights(prices, n_clusters = prices.shape[1]-1)
    print("Weights")
    print(weights)
    print("Setting Data Frame")
    weights = pd.DataFrame(weights).T
    print("Setting index")
    weights.index = [end_date]
    weights = weights.reindex(sorted(weights.columns), axis = 1)
    print("Saving weights to storage")
    #today = datetime.now().strftime("%Y-%m-%d")
    filename = "/tmp/HRP_weights_{}.csv".format(end_date)
    weights.to_csv(filename)
    print("uploading to bucket")
    uploadToBucket('face_recog_bd',source_path=filename, destination_path=filename.split('/')[-1])
    return weights.to_json(orient='split')


#
#today
#ada = getDailyBars("X:ADAUSD", "2019-01-01", today)






