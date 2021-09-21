import json
import time
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
#from utils import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, IntegerType
from collections import defaultdict
from scipy.spatial import KDTree
from external_functions import *


CELL_AREA    = '/data/cell_area.csv'
PARQUETS_DIR = '/data/parquets'
CURRENT_DIR  = '/home/hellscream/Documents/backendSpark'

with open('tow-cell.json') as json_file:
    tow_cell_mapp = json.load(json_file)

mapps = {}

'''
root
 |-- code: integer (nullable = true)
 |-- cell_ids: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- times: array (nullable = true)
 |    |-- element: long (containsNull = true)
'''

def init_spark_session(name='Mobility'):
    spark = SparkSession.builder.appName(name).getOrCreate()
    #calculate_static_mappings(spark)
    return spark

def calculate_static_mappings(spark):
    global mapps
    
    dic_full_df = spark.read.format('csv').options(header='true', delimiter='\t')\
                    .load(CURRENT_DIR + CELL_AREA)\
                    .select('*')
                    
    dic_full_pandas = dic_full_df.toPandas()
    dic_full = dic_full_pandas.to_dict('index')
    
    dic_tow_cell = {}
    for i in dic_full:
        dic_tow_cell[dic_full[i]['id']] = dic_full[i]['area_correlator'] 
    
    dic_cell_latlon = {}
    for i in dic_full:
        dic_cell_latlon[dic_full[i]['area_correlator']] = [dic_full[i]['latitude'], dic_full[i]['longitude']]

    mapps['tow-cell'] = dic_tow_cell
    mapps['cell-latlon'] = dic_cell_latlon

    with open('tow-cell.json', 'w') as outfile:
        json.dump(dic_tow_cell, outfile)


    with open('cell-latlon.json', 'w') as outfile:
        json.dump(dic_cell_latlon, outfile)


######################################################################
############################## Mobility ##############################
######################################################################

def mobility(spark_session, date, time_start_lower, time_start_high, time_end_lower, time_end_high):
    users_cells_start = get_mobility_at_time_interval(spark_session, date, time_start_lower, time_start_high) 
    users_cells_end = get_mobility_at_time_interval(spark_session, date, time_end_lower, time_end_high)
    matrix = build_matrix(date, users_cells_start, users_cells_end)

    with open('2021-03-01.json', 'w') as file:
        json.dump(matrix, file)

def get_mobility_at_time_interval(spark, date, time_start, time_end):
    time_start *= 3600
    time_end *= 3600

    # load correspondent parquet
    df = spark.read.parquet(CURRENT_DIR + PARQUETS_DIR + '/' + date)
    
    # get registers between time spam
    df = df.rdd.flatMap(lambda rows : get_range(rows, time_start, time_end))\
            .toDF(['code', 'towers', 'times']).select('*').filter(F.size(F.col('times')) > 0)

    # map tower_id to cell_id
    df = df.rdd.flatMap(lambda x : mapp_tow_cell(x, tow_cell_mapp)).toDF(['code', 'towers', 'times'])

    # count distinct ocurrences of every cell and normalize
    count_occurrences_udf = F.udf(lambda x : count_occurrences_and_normalize(x), ArrayType(ArrayType(StringType())))
    df = df.select('code', count_occurrences_udf(F.col('towers')).alias('towers-count'))

    return df


def build_matrix(date, users_cells_start, users_cells_end):
    union_users_cells = users_cells_start.union(users_cells_end)
    
    union_users_cells = union_users_cells.groupBy('code')\
                        .agg(F.collect_list('towers-count').alias('cells'), F.count('code').alias('count'))\
                        .filter(F.col('count') == 2)

    rdd = union_users_cells.select('cells').rdd.flatMap(lambda x : flat_origin_destination_product(x))
    df = rdd.toDF(['start', 'end', 'value'])
    df = df.groupBy('start').pivot('end').agg(F.sum('value')).na.fill(value=0)

    matriz_pandas = df.toPandas()
    
    m = fix_json_format(json.loads(matriz_pandas.to_json(orient='index')))
    m_pandas = pd.DataFrame(m)
    m_transpose = m_pandas.transpose().to_dict()

    matrix = {
			'type': 'matrix',
			'labels': ['interaction'],
			'date': date,
			'matrix': m,
			'matrixTranspose': m_transpose,
		}
    
    return matrix

def test_mobility_matrix():
    spark_session = init_spark_session()
    mobility(spark_session, '2021-03-01', 6, 10, 16, 20)

if __name__ == '__main__':
    test_mobility_matrix()