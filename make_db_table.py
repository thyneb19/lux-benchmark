import pandas as pd
import psycopg2
import csv

from sqlalchemy import create_engine
table_name = "airbnb_50x"
data = pd.read_csv("data/airbnb_50x.csv")
engine = create_engine("postgresql://postgres:lux@localhost:5432")
print("creating ", table_name)
data.to_sql(name=table_name, con=engine, if_exists="replace", index=False)