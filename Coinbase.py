import pandas as pd 
import numpy as np
import requests
from urllib.request import urlopen
import json
import csv
from datetime import datetime, timedelta
import ast
import pytz
import os


class Coinbase:
    def __init__(self, path=None):
        self.path = path
        self.pairs,self.starts,self.ends = self.reader()
        # self.starts_ends = self.str2dt()
        self.api = "https://api.pro.coinbase.com/products/{}/candles?start={}&end={}&granularity=3600"
        # self.urls = self.getUrl()
        self.request()

    def epoch2utc(self, epoch):
        return datetime.fromtimestamp(epoch,pytz.timezone("UTC"))

    def str2dt(self,starts_ends):
        convert = lambda x: datetime.strptime(x, '%m/%d/%Y').replace(tzinfo=pytz.timezone('US/Eastern'),microsecond=0).isoformat()
        starts = [convert(i[0]) for i in starts_ends]
        ends = [convert(i[1]) for i in starts_ends]
        arr = [(start,end) for (start,end) in zip(starts,ends)]
        return arr


    def reader(self):
        df = pd.read_csv(self.path+"/list/Coinbase.csv" if self.path else ".\\list\\Coinbase.csv", header = None, index_col = False)
        pairs, starts, ends = df[0], df[1], df[2]
        pairs = [i.replace("/","-") for i in pairs]
        return pairs, starts, ends
        

    def getUrl(self,pairs,starts_ends):
        urls = []
        if len(pairs) == len(starts_ends):
            for (x,y) in zip(pairs, starts_ends):
                urls.append(self.api.format(x,y[0],y[1]))
        else:
            """
            one pair with multiple partitions
            """
            for y in starts_ends:
                urls.append(self.api.format(pairs,y[0],y[1]))
        return urls

    def request(self):
        header =  {0:"Epoch Time",1:"Open", 2:"High", 3:"Low", 4:"Close", 5:"Volume"}
        for (start_end,pair) in zip(zip(self.starts, self.ends),self.pairs):
            # print(start_end)
            df_lst = []
            batchlst = self.partition(start_end)
            urls = self.getUrl(pair,batchlst)
            # print(urls)
            for url in urls:
                re = requests.get(url)
                data = re.json()
                df = pd.DataFrame(data,columns=header)
                df_lst.append(df)
            merged = pd.concat(df_lst)
            self.writer(merged,pair)
                


    def writer(self, df, pair):
        header =  {0:"Epoch Time",1:"Open", 2:"High", 3:"Low", 4:"Close", 5:"Volume"}
        df = df.rename(columns = header)
        # print(df.head())
        df["Date (UTC)"] = df["Epoch Time"].apply(lambda i : self.epoch2utc(i).date())
        df["Time (UTC)"] = df["Epoch Time"].apply(lambda i : self.epoch2utc(i).time())
        # df["Epoch Time"] = df["Epoch Time"].apply(lambda i : str(int(i)))
        if not os.path.exists('Coinbase'):
            os.makedirs('Coinbase')
        df.sort_values('Epoch Time',ascending=False).to_excel(self.path+"/Coinbase/{}.xlsx".format(pair) if self.path else ".\\Coinbase\\{}.xlsx".format(pair), index = False)


    def partition(self,start_end):
        """
        due to limit 300 per request, need to partition the timeframe, using %300 to track the last few, mini_batch = [300 hours]
        """
        mini_nbatch = 300
        d1 = datetime.strptime(start_end[0],'%m/%d/%Y')
        d2 = datetime.strptime(start_end[1],'%m/%d/%Y')
        # print(d2-d1)
        diff_h = (d2 - d1).total_seconds() / (60*60)
        # print(diff_h)
        n_batch = int(diff_h / mini_nbatch)
        reminder = diff_h % mini_nbatch
        # print(n_batch, reminder)
        convert = lambda x: (timedelta(hours = x*mini_nbatch)+d1).replace(tzinfo=pytz.timezone('US/Eastern'),microsecond=0).isoformat()
        mini_batch = [(convert(i), convert(i+1)) for i in np.arange(n_batch,dtype = np.float64)]
        mini_batch.append(((d2-timedelta(hours = reminder)).replace(tzinfo=pytz.timezone('US/Eastern'),microsecond=0).isoformat(), d2.replace(tzinfo=pytz.timezone('US/Eastern'),microsecond=0).isoformat()))
        # print(mini_batch[0][1])
        return mini_batch



if __name__ == '__main__':
    tmp = Coinbase()


# pairs = ["BCH-USD"]
# starts_ends = [("1/1/2020","12/1/2020"),("1/1/2019","5/1/2019")]
# out = tmp.getUrl(*pairs,starts_ends)
# print(out)

# part = tmp.partition(*starts_ends)
# print(part)