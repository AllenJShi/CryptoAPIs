import pandas as pd 
import numpy as np
import requests
from urllib.request import urlopen
import json
import csv
import os
"""
this a class for Bittrex API
"""

class Bittrex:
    def __init__(self,path = None):
        self.path = path
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.time = None
        self.volume = None
        self.book_value = None
        self.urls, self.pairs = self.getUrls()
        self.market = None
        self.tickInterval = None
        self.run()


    def getPrice(self,url):
        response = requests.get(url)
        data = json.load(urlopen(url))
        result = None
        if data["success"]:
            # print(data["success"])
            result = data["result"]
            # print(type(result))
        return result

    def parser(self, result, pair):
        """
        this method should create a csv file and store a list of price
        """
        header = {"O":"Open", "H":"High", "L":"Low", "C":"Close", "V":"Volume", "T":"Time" ,"BV":"Base Value"}
        df = pd.DataFrame(result)
        df = df.rename(columns = header)
        df["Date (UTC)"] = df["Time"].apply(lambda i: i.split("T")[0])
        df["Time (UTC)"] = df["Time"].apply(lambda i: i.split("T")[1])
        df = df.drop(["Time"], axis = 1)
        if not os.path.exists('Bittrex'):
            os.makedirs('Bittrex')
        df.to_csv(self.path + "/Bittrex/{}.csv".format(pair) if self.path else ".\\Bittrex\\{}.csv".format(pair), index = False)

    def getUrls(self):
        """
        return a list of URL
        """
        pair = pd.read_csv(self.path + "/list/Bittrex.csv" if self.path else ".\\list\\Bittrex.csv", header = None, index_col = False)
        urls = []
        pairs = []
        for i in range(len(pair)):
            urls.append("https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName={}-{}&tickInterval=hour".format(pair[0][i],pair[1][i]))
            pairs.append((pair[0][i],pair[1][i]))
        return urls,pairs


    def run(self):
        for (url,pair) in zip(self.urls, self.pairs):
            result = self.getPrice(url)
            try:
                self.parser(result, pair)
                print(pair)
            except:
                print(pair, "not found")
                continue        
    


def main():
    temp = Bittrex()
    for (url,pair) in zip(temp.urls, temp.pairs):
        result = temp.getPrice(url)
        try:
            temp.parser(result, pair)
            print(pair)
        except:
            print(pair, "not found")
            continue
        


if __name__ == "__main__":
    # execute only if run as a script
    main()