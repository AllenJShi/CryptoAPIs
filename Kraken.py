import pandas as pd 
import numpy as np
import requests
from urllib.request import urlopen
import json
import csv
from datetime import datetime
import os
import ast


class Kraken:
    def __init__(self):
        self.urls, self.pairs = self.getUrls()


    def epoch2est(self, epoch):
        return datetime.fromtimestamp(epoch)

    def getAPI(self, url, pair):
        response = urlopen(url)
        text = response.read()
        
        # data = json.dumps(text.decode()).replace("'", '"')[1:-1]
        # # print(type(data))
        # x = bytes(data, encoding='utf-8')
        # print((x))
        # t=json.loads(x.decode('utf-8')) 
        # print(type(t))
        # # print(t)

        json_data = ast.literal_eval(json.dumps(text.decode()))
        dat = json.loads(json_data)
        tmp = dat["result"]
        data = tmp[list(tmp.keys())[0]]
        df = pd.DataFrame(data)
        self.writer(df, pair)


    def writer(self, df, pair):
        # header = {0:"Time",1:"Open", 2:"High", 3:"Low", 4:"Close", 5:"Volume"}
        # df = df.rename(columns = header)
        df[0] = df[0].apply(lambda i : self.epoch2est(i))
        df.to_csv(".\\Kraken\\{}.csv".format(pair), index = False)

    def getUrls(self):
        pair = pd.read_csv(".\\list\\Kraken.csv", header = None, index_col = False)
        pairs = [i.replace("/","") for i in pair[0]]
        # print(pair)
        urls = []
        for i in pairs:
            urls.append("https://api.kraken.com/0/public/OHLC?pair={}&interval=60&since=0".format(i))
        return urls,pairs


temp = Kraken()
# df = temp.getAPI()


# temp.writer(df)
# temp.getUrls()

for (url,pair) in zip(temp.urls,temp.pairs):
    dat = temp.getAPI(url,pair)