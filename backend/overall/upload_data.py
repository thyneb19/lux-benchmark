import pandas as pd
import csv
import numpy as np
from sqlalchemy import create_engine

def create_airbnb_table(table_size = 500000, table_name=""):
    data = pd.read_csv("https://raw.githubusercontent.com/lux-org/lux-datasets/master/data/airbnb_nyc.csv")
    data = data.sample(table_size, replace=True)
    engine = create_engine("postgresql://postgres:lux@localhost:5432")

    data.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

trial_range = np.geomspace(500000, 2.2e6, num=10,dtype=int)
i = 1
for nPts in trial_range:
    tbl_name = "benchmarking_"+str(i)
    print(tbl_name)
    create_airbnb_table(table_size = nPts, table_name = tbl_name)
    i = i+1