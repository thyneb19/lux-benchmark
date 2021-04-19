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
import pandas as pd
from lux.vis.Vis import Vis

from sqlalchemy import Table, MetaData
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy_views import CreateView, DropView

# trial_range = np.geomspace(5e3, 199000, num=3,dtype=int)
trial_range = np.geomspace(500000, 2.2e6, num=10,dtype=int)
trial = []  # [cell count, duration]
# Must turn off sampling, otherwise maintain_rec constant cost
lux.config.sampling = False
lux.config.lazy_maintain = True
# lux.config.plotting_backend="matplotlib"
engine = create_engine("postgresql://postgres:lux@localhost:5432/postgres")
lux.config.set_SQL_connection(engine)

original_df = pd.read_csv("https://raw.githubusercontent.com/lux-org/lux-datasets/master/data/airbnb_nyc.csv")

for i in range(1, 11):
    nPts = trial_range[i-1]
    tbl_name = "benchmarking_"+str(i)
    print("benchmarking with {} with {} rows".format(tbl_name, nPts))
    #test SQLExecutor with Postgres view
    # view = Table('benchmarking', MetaData())
    # drop_view = DropView(view, if_exists=True, cascade=True)

    # view_string = "SELECT * FROM airbnb_50x ORDER BY Random() LIMIT {}".format(nPts)
    # definition = text(view_string)
    # create_view = CreateView(view, definition)

    # with engine.connect() as connection:
    #     result = connection.execute(drop_view)
    #     result = connection.execute(create_view)

    # print("view created")

    #test SQLExecutor with Postgres table
    # data = original_df.sample(nPts, replace=True)
    # data.to_sql(name="benchmarking_tbl", con=engine, if_exists="replace", index=False)
    # print("table created")

    tbl = lux.LuxSQLTable(table_name = tbl_name)
    print("LuxSQLTable instantiated")
    # Warm start
    vis = Vis(['latitude','longitude'], tbl)
    ################# Regular Scatterplot ############################
    quantitative=['latitude', 'longitude', 'price', 'minimum_nights', 'number_of_reviews', 'reviews_per_month', 'calculated_host_listings_count']
    if (experiment=="scatter"):
        lux.config.heatmap = False
        test_scatter = [['latitude','longitude'], ['price','longitude'],['price','minimum_nights'],['number_of_reviews','minimum_nights'],['number_of_reviews','price']]
        for test in test_scatter:
            print("current test: ", test)
            start = time.perf_counter()
            vis = Vis(test, tbl)
            end = time.perf_counter()
            t = end - start
            trial.append([nPts,t,test[0],test[1]])
    ################# Color Scatterplot ############################
    elif (experiment=="colorscatter"):
        lux.config.heatmap = False
        for attr in ['host_id', 'host_name', 'neighbourhood_group','neighbourhood', 'room_type', 'number_of_reviews']:
            start = time.perf_counter()
            vis = Vis(['price','minimum_nights',lux.Clause(attr,channel="color")], tbl)
            end = time.perf_counter()
            t = end - start
            trial.append([nPts,t,attr])
    ################# Regular Histogram ############################
    elif (experiment=="histogram"):
        for b in list(range(5,205,10)):
            start = time.perf_counter()
            vis = Vis([lux.Clause("number_of_reviews",bin_size=b)], tbl)
            end = time.perf_counter()
            t = end - start
            trial.append([nPts,t,b])

    # ################# Regular bar ############################
    elif (experiment=="bar"):
        for attr in ["room_type", "neighbourhood_group","neighbourhood","host_name","name"]:
            G_axes = tbl.cardinality[attr]
            start = time.perf_counter()
            vis = Vis([attr, "number_of_reviews"], tbl)
            end = time.perf_counter()
            t = end - start
            trial.append([nPts,t,G_axes,0])

    elif (experiment=="colorbar"):
        ################# Bar  Vary Axes Group Cardinality ############################
        for attr in ["room_type","neighbourhood_group","neighbourhood","host_name"]:
            G_axes = tbl.cardinality[attr]
            for cattr in ["room_type","neighbourhood_group","neighbourhood","host_name"]:
                if cattr!=attr:
                    start = time.perf_counter()
                    G_color = tbl.cardinality[cattr]
                    vis = Vis([lux.Clause(attr,channel="y"), "number_of_reviews",lux.Clause(cattr,channel="color")], tbl)
                    end = time.perf_counter()
                    t = end - start
                    trial.append([nPts,t,G_axes,G_color])
    # ################# Heatmap ############################
    elif (experiment=="heatmap"):
        binlist = np.geomspace(5,500,10,dtype=int)
        lux.config.heatmap = True
        for b in binlist:
            lux.config.heatmap_bin_size = b
            start = time.perf_counter()
            vis = Vis(["longitude","latitude"],tbl)
            end = time.perf_counter()
            t = end - start
            trial.append([nPts,"heatmap",t,b])

            start = time.perf_counter()
            vis = Vis(["longitude","latitude",'price'],tbl)
            end = time.perf_counter()
            t = end - start
            trial.append([nPts,"quantitative color heatmap",t,b])
            
            start = time.perf_counter()
            vis = Vis(["longitude","latitude",'neighbourhood_group'],tbl)
            end = time.perf_counter()
            t = end - start
            trial.append([nPts,"categorical color heatmap",t,b])
    
    print(f"Completed {nPts}")
if (experiment=="heatmap"):
    trial_tbl = pd.DataFrame(
        trial,
        columns=["nPts","mark","time","nbin"]
    )
elif (experiment=="histogram"):
    trial_tbl = pd.DataFrame(
        trial,
        columns=["nPts","time","nbin"]
    )
elif ("bar" in experiment):
    trial_tbl = pd.DataFrame(
        trial,
        columns=["nPts","time","G_axes","G_color"]
    )
elif (experiment=="scatter"):
    trial_tbl = pd.DataFrame(
        trial,
        columns=["nPts","time","attr1","attr2"]
    )
elif (experiment=="colorscatter" or experiment == "selection"):
    trial_tbl = pd.DataFrame(
        trial,
        columns=["nPts","time","attr"]
    )
trial_tbl.to_csv(f"{result_dir}costmodel_sql_tbl{experiment}.csv", index=None)

