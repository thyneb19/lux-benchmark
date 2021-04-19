import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:lux@localhost:5432/postgres")

data = pd.read_csv('../lux-benchmark/data/airbnb_50x.csv')
data.to_sql(name='airbnb_50x', con=engine, if_exists = 'replace', index=False)