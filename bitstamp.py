import pandas as pd 
import numpy as np 
from datetime import datetime
import pytz


class BitStamp:
    def __init__(self, path =  None):
        if path is None:
            self.path = ".\\XRPUSD_transactions.csv" #XRPUSD_transactions
        self.dat = self.reader()
        self.df = self.convert2utc(self.dat)

    def reader(self, path = None):
        if path is None:
            path = self.path
        dat = pd.read_csv(path)
        return dat

    def epoch2utc(self, epoch):
        return datetime.fromtimestamp(epoch,pytz.timezone("UTC"))

    def convert2utc(self, df = None):
        if df is None:
            df = self.dat
        df["date (UTC)"] = df["timestamp"].apply(lambda i : self.epoch2utc(i).date())
        df["time (UTC)"] = df["timestamp"].apply(lambda i : self.epoch2utc(i).time())
        df["year (UTC)"] = df["timestamp"].apply(lambda i : self.epoch2utc(i).year)
        """
        2020 data
        """
        df = df.loc[df["year (UTC)"] == 2020]

        return df

    def writer(self, df = None):
        if df is None:
            df = self.df
        df.to_csv("last_trade.csv", index =  False)

    def search(self, df = None):
        if df is None:
            df =  self.df

        df["hour"] = df["time (UTC)"].apply(lambda i :  i.hour)
        df = df.reset_index(drop=True)
        print(df)
        """
        if next hour change or null, then take the current entry; if not, keep going
        """
        lst = []
        ind = df.index
        for i in ind:
            if i+1 in ind and df.iloc[i]["hour"] == df.iloc[i+1]["hour"]:
                continue
            else:
                lst.append(df.iloc[i])
        out = pd.DataFrame(lst, columns = df.columns)
        return out

temp = BitStamp()
out = temp.search()
temp.writer(out)

# date_obj = datetime.strptime("January 1 2020  0:00AM",'%b %d %Y %I:%M%p')
# print(date_obj)