"""
This file is used for benchmarking the performance for each Vis type
"""
import sys, os

sys.path.append(os.path.abspath("."))
sys.path.insert(0, 'C:/Users/thyne/Documents/GitHub/lux')

experiment = sys.argv[1]#"scatter"
#  python -i backend/overall/cost_estimation_model.py colorscatter
print (f"Starting {experiment}")
result_dir = "result/"

import time
import numpy as np
import lux
from lux.executor.SQLExecutor import SQLExecutor
import pandas as pd
from lux.vis.Vis import Vis

from sqlalchemy import Table, MetaData
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:lux@localhost:5432/postgres")
lux.config.set_SQL_connection(engine)
trial = []
trial_range = np.geomspace(500000, 2.2e6, num=10,dtype=int)

executor = SQLExecutor()
for i in range(1, 11):
	nPts = trial_range[i-1]
	tbl = lux.LuxSQLTable()
	tbl_name = "benchmarking_"+str(i)
	tbl.table_name = tbl_name

	if experiment == "attributes":
		print("retrieving attributes for {}".format(tbl_name))
		start = time.perf_counter()
		SQLExecutor.get_SQL_attributes(SQLExecutor, tbl)
		end = time.perf_counter()
		print(end-start)
		print("number of columns: ", len(tbl.columns))
		t = end - start
		trial.append([nPts,t])
	elif experiment == "unique":
		executor.get_SQL_attributes(tbl)
		print("retrieving unique values for {}".format(tbl_name))
		start = time.perf_counter()
		executor.get_unique_values(tbl)
		end = time.perf_counter()
		print(end-start)
		t = end - start
		trial.append([nPts,t])
	elif experiment == "cardinality":
		executor.get_SQL_attributes(tbl)
		print("retrieving cardinality for {}".format(tbl_name))
		start = time.perf_counter()
		executor.get_cardinality(tbl)
		end = time.perf_counter()
		print(end-start)
		t = end - start
		trial.append([nPts,t])
	elif experiment == "stats":
		executor.get_SQL_attributes(tbl)
		executor.compute_data_type(tbl)
		print("retrieving stats for {}".format(tbl_name))
		start = time.perf_counter()
		executor.compute_stats(tbl)
		end = time.perf_counter()
		print(end-start)
		t = end - start
		trial.append([nPts,t])
	elif experiment == "datatype":
		executor.get_SQL_attributes(tbl)
		print("retrieving datatype for {}".format(tbl_name))
		start = time.perf_counter()
		executor.compute_data_type(tbl)
		end = time.perf_counter()
		print(end-start)
		t = end - start
		trial.append([nPts,t])
	print(f"Completed {nPts}")

trial_tbl = pd.DataFrame(
	trial,
	columns=["nPts","time"]
)

trial_tbl.to_csv(f"{result_dir}metadata_sql_tbl_{experiment}.csv", index=None)
