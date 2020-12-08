import pandas as pd 
import numpy as np
import requests
from urllib.request import urlopen
import json
import csv
from datetime import datetime
import os

class Gemini:
    def __init__(self):
        self.urls, self.pairs = self.getUrls()

    def epoch2est(self, epoch):
        return datetime.fromtimestamp(epoch/1000)

    def getAPI(self, url, pair):
        response = requests.get(url)
        data = json.load(urlopen(url))
        # print(type(data))
        if data:
            df = pd.DataFrame(data)
            self.writer(df, pair)


    def getUrls(self):
        pair = pd.read_csv(".\\list\\Gemini.csv", header = None, index_col = False)
        # print(type(pair))
        pairs = [i.replace("/","") for i in pair[0]]
        # print(pair)
        urls = []
        for i in pairs:
            urls.append("https://api.gemini.com/v2/candles/{}/1hr".format(i))
        return urls,pairs

    def writer(self, df, pair):
        header =  {0:"Time",1:"Open", 2:"High", 3:"Low", 4:"Close", 5:"Volume"}
        df = df.rename(columns = header)
        df["Time"] = df["Time"].apply(lambda i : self.epoch2est(i))
        df.to_csv(".\\Gemini\\{}.csv".format(pair), index = False)

temp = Gemini()

for (url,pair) in zip(temp.urls,temp.pairs):
    dat = temp.getAPI(url,pair)