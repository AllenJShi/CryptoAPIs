import os
import glob
import pandas as pd

exchange = "Kraken"

os.chdir("../FLLP/{}".format(exchange))

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

combined_csv = pd.DataFrame()

for f in all_filenames:
    df = pd.read_csv(f, index_col=False)
    df["Pair"] = f.split(".")[0]
    combined_csv = pd.concat([combined_csv, df])

combined_csv.to_csv( "{}.csv".format(exchange), index=False, encoding='utf-8-sig')