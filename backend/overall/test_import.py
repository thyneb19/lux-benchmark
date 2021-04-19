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
import utils
from lux.vis.Vis import Vis

from sqlalchemy import Table, MetaData
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy_views import CreateView, DropView