import pandas as pd 
import numpy as np
import requests
from urllib.request import urlopen
import json
import csv

"""
this a class for Bittrex API
"""

class Bittrex:
    def __init__(self):
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
        df.to_csv('.\\Bittrex\\{}-{}.csv'.format(pair[0],pair[1]), index = False)

    def getUrls(self):
        """
        return a list of URL
        """
        pair = pd.read_csv(".\\list\\Bittrex.csv", header = None)
        urls = []
        pairs = []
        for i in range(len(pair)):
            urls.append("https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName={}-{}&tickInterval=hour".format(pair[0][i],pair[1][i]))
            pairs.append((pair[0][i],pair[1][i]))
        return urls,pairs

    
def main():
    temp = Bittrex()
    for (url,pair) in zip(temp.urls, temp.pairs):
        result = temp.getPrice(url)
        temp.parser(result, pair)
        


if __name__ == "__main__":
    # execute only if run as a script
    main()