import pandas as pd 
import numpy as np
import requests
from urllib.request import urlopen
import json
import csv
from datetime import datetime, timedelta
import ast
import pytz


class Binance:
    def __init__(self):
        self.api = "https://api.binance.com/api/v1/klines?symbol={}&interval=1h&startTime={}&endTime={}"
        self.pairs,self.starts_ends = self.reader()
        self.request()

    def reader(self):
        df = pd.read_csv(".\\list\\BinanceUS.csv", header = None, index_col = False)
        pairs, starts, ends = df[0], df[1], df[2]
        pairs = [i.replace("/","") for i in pairs]
        starts_ends = [(self.utc2epoch(start),self.utc2epoch(end)) for (start,end) in zip(starts,ends)]
        return pairs, starts_ends

        """
        https://api.binance.com/api/v1/klines?symbol=AIONBTC&interval=1h&startTime=1514750400000&endTime=1514754000000
        """

    def getUrl(self,pairs,starts_ends):
        urls = []
        for (x,y) in zip(pairs, starts_ends):
            print(y)
            urls.append(self.api.format(x,y[0],y[1]))
        return urls

    def epoch2utc(self, epoch):
        return datetime.fromtimestamp(epoch,pytz.timezone("UTC"))

    def utc2epoch(self, utc):
        mdy = [int(char) for char in utc.split("/")]
        timestamp = int(datetime(mdy[2], mdy[0], mdy[1], 0, 0).timestamp())*1000
        return timestamp

    def request(self):
        header =  {0:"Epoch",1:"Open", 2:"High", 3:"Low", 4:"Close", 5:"Volume", 6:"6",7:"7",8:"8",9:"9",10:"10",11:"11"}
        urls = self.getUrl(pairs = self.pairs,starts_ends = self.starts_ends)
        for (url,pair) in zip(urls,self.pairs):
            re = requests.get(url)
            data = re.json()
            df = pd.DataFrame(data)
            df = df.rename(columns = header)
            self.writer(df,pair)

    def writer(self, df, pair):
        df["Date (UTC)"] = (df["Epoch"]/1000).apply(lambda i : self.epoch2utc(i).date())
        df["Time (UTC)"] = (df["Epoch"]/1000).apply(lambda i : self.epoch2utc(i).time())
        df.to_csv(".\\Binance\\{}.csv".format(pair), index = False)

obj = Binance()