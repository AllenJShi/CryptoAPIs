import pandas as pd 
import numpy as np
import requests
from urllib.request import urlopen
import json
import csv
from datetime import datetime
import os
import pytz

class Gemini:
    def __init__(self,path=None):
        self.path = path
        self.urls, self.pairs = self.getUrls()
        self.run()

    def epoch2utc(self, epoch):
        return datetime.fromtimestamp(epoch/1000,pytz.timezone("UTC"))

    def getAPI(self, url, pair):
        response = requests.get(url)
        data = json.load(urlopen(url))
        # print(type(data))
        if data:
            df = pd.DataFrame(data)
            self.writer(df, pair)


    def getUrls(self):
        pair = pd.read_csv(self.path+"/list/Gemini.csv" if self.path else ".\\list\\Gemini.csv", header = None, index_col = False)
        # print(type(pair))
        pairs = [i.replace("/","") for i in pair[0]]
        # print(pair)
        urls = []
        for i in pairs:
            urls.append("https://api.gemini.com/v2/candles/{}/1hr".format(i))
        return urls,pairs

    def writer(self, df, pair):
        header =  {0:"Epoch Time",1:"Open", 2:"High", 3:"Low", 4:"Close", 5:"Volume"}
        df = df.rename(columns = header)
        df["Date (UTC)"] = df["Epoch Time"].apply(lambda i : self.epoch2utc(i).date())
        df["Time (UTC)"] = df["Epoch Time"].apply(lambda i : self.epoch2utc(i).time())
        df["Epoch Time"] = df["Epoch Time"].apply(lambda i : str(int(i)))
        if not os.path.exists("Gemini"):
            os.mkdir("Gemini")
        df.to_csv(self.path+"/Gemini/{}.csv".format(pair) if self.path else ".\\Gemini\\{}.csv".format(pair), index = False)


    def run(self):
        for (url,pair) in zip(self.urls,self.pairs):
            dat = self.getAPI(url,pair)        

def main():
    temp = Gemini()
    for (url,pair) in zip(temp.urls,temp.pairs):
        dat = temp.getAPI(url,pair)

if __name__ == '__main__':
    main()