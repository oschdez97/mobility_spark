import json
import time
import math
import numpy as np
from os import path
import pandas as pd
import pyspark.sql.functions as F
#from utils import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, IntegerType
from collections import defaultdict
from scipy.spatial import KDTree
from external_functions import *

import warnings
warnings.filterwarnings('ignore')

CURRENT_DIR = path.join(path.dirname(path.abspath(__file__)))
CELL_AREA    = CURRENT_DIR + '/data/cell_area.csv'
PARQUETS_DIR = CURRENT_DIR + '/data/parquets'
TOWER_AUX_PROPERTIES_FILE = CURRENT_DIR + '/data/voronoi_props.json'
MAPP_CELL_TOW_DIR = CURRENT_DIR + '/data/cell-tow.json'
MAPP_TOW_LATLON_DIR = CURRENT_DIR + '/data/tow-latlon.json'

with open(MAPP_CELL_TOW_DIR) as json_file:
    cell_tow_mapp = json.load(json_file)

with open(MAPP_TOW_LATLON_DIR) as json_file:
    tow_latlon_mapp = json.load(json_file)


# DF Schema
'''
root
 |-- code: integer (nullable = true)
 |-- cell_ids: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- times: array (nullable = true)
 |    |-- element: long (containsNull = true)
'''

# do not mistake
# tower : dhj7w9
# cell  : 171-28271

def init_spark_session(name='Mobility'):
    spark = SparkSession.builder.appName(name).getOrCreate()
    return spark


map_area_correlator_to_coord_udf = F.udf(lambda x : map_area_correlator_to_coord(x, tow_latlon_mapp)\
        ,ArrayType(ArrayType(StringType())))

######################################################################
############################## Mobility ##############################
######################################################################

def mobility(date, time_start_lower, time_start_high, time_end_lower, time_end_high):
    
    spark_session = init_spark_session()

    users_cells_start, workers = get_mobility_at_time_interval(spark_session, date, time_start_lower, time_start_high) 
    users_cells_end, sleepers = get_mobility_at_time_interval(spark_session, date, time_end_lower, time_end_high)
    sleepers_v2 = get_sleepers(spark_session, date)
    matrix, vector = build_matrix(date, users_cells_start, users_cells_end, workers, sleepers, sleepers_v2)
    
    return matrix, vector

    '''
    with open('2021-03-01_matrix.json', 'w') as file:
        json.dump(matrix, file)

    with open('2021-03-01_vector.json', 'w') as file:
        json.dump(vector, file)
    '''

def get_mobility_at_time_interval(spark, date, time_start, time_end):
    time_start = get_time_in_seconds(time_start)
    time_end = get_time_in_seconds(time_end)

    # load correspondent parquet
    df = spark.read.parquet(PARQUETS_DIR + '/' + date)
    
    # get registers between time spam
    df = df.rdd.flatMap(lambda rows : get_range(rows, time_start, time_end))\
            .toDF(['code', 'towers', 'times']).select('*').filter(F.size(F.col('times')) > 0)

    # map cell_id to tower_id
    df = df.rdd.flatMap(lambda x : mapp_cell_tow(x, cell_tow_mapp)).toDF(['code', 'towers', 'times'])

    # count distinct ocurrences of every cell and normalize
    count_occurrences_udf = F.udf(lambda x : count_occurrences_and_normalize(x), ArrayType(ArrayType(StringType())))
    df = df.select('code', count_occurrences_udf(F.col('towers')).alias('towers-count'))

    # tower -> count: workers/sleepers depending of the given role 
    user_tower_role = df.rdd.flatMap(lambda row : ((tow, val) for tow, val in row[1])).toDF(['tower', 'val'])
    user_tower_role = user_tower_role.groupBy('tower').agg(F.sum('val').alias('sum')).toPandas().to_dict(orient='list')
    user_tower_role = dict(zip(user_tower_role['tower'], user_tower_role['sum']))

    return df, user_tower_role


def get_sleepers(spark, date, time_sleep_lower='01:00', time_sleep_high='04:00'):
    time_sleep_lower = get_time_in_seconds(time_sleep_lower)
    time_sleep_high = get_time_in_seconds(time_sleep_high)
    
    # load correspondent parquet
    df = spark.read.parquet(PARQUETS_DIR + '/' + date)

    # map cell_id to tower_id
    users_cells_sleep = df.rdd.flatMap(lambda x : mapp_cell_tow(x, cell_tow_mapp)).toDF(['code', 'towers', 'times'])
    
    # count distinct ocurrences of every cell
    users_cells_sleep = users_cells_sleep.rdd.flatMap(lambda x : accumulate_count(x, time_sleep_lower, time_sleep_high))\
        .toDF(['code', 'towers', 'count'])
    users_cells_sleep = users_cells_sleep.select('*').where(F.size('towers') > 0)

    #map cell_id to latlon coords
    #map_area_correlator_to_coord_udf = F.udf(lambda x : map_area_correlator_to_coord(x, tow_latlon_mapp), ArrayType(ArrayType(StringType())))
    users_cells_sleep = users_cells_sleep.withColumn('coords', map_area_correlator_to_coord_udf(F.col('towers'))).select('*')

    # getting mean and home tower
    normalize_udf = F.udf(lambda values : [val / sum(values) for val in values], ArrayType(StringType()))
    mean_home_tower_udf = F.udf(lambda weight, coords : get_mean_home_tower(weight, coords), ArrayType(StringType()))
    
    # imsi -> spleep_area
    users_cells_sleep = users_cells_sleep.withColumn('weight', normalize_udf(F.col('count')))\
                                .select('code', mean_home_tower_udf(F.col('weight'), F.col('coords')).alias('home_tower'))
    
    # area_correlator -> count
    coord_area_correlator_mapp = {str(v): k for k, v in tow_latlon_mapp.items()}
    map_coord_to_area_correlator_udf = F.udf(lambda x : map_coord_to_area_correlator(x, coord_area_correlator_mapp), StringType())
    res = users_cells_sleep.select(map_coord_to_area_correlator_udf(F.col('home_tower')).alias('area_correlator'))
    res = res.groupBy('area_correlator').count()
    
    res = res.toPandas().to_dict(orient='list')
    return dict(zip(res['area_correlator'], res['count']))
    

def build_matrix(date, users_cells_start, users_cells_end, workers, sleepers, sleepers_v2):
    union_users_cells = users_cells_start.union(users_cells_end)
    
    union_users_cells = union_users_cells.groupBy('code')\
                        .agg(F.collect_list('towers-count').alias('cells'), F.count('code').alias('count'))\
                        .filter(F.col('count') == 2)

    rdd = union_users_cells.select('cells').rdd.flatMap(lambda x : flat_origin_destination_product(x))
    df = rdd.toDF(['start', 'end', 'value'])

    tower_user_displacement_distance = df.rdd.flatMap(lambda row : distance_displacement(row, tow_latlon_mapp)).toDF(['start', 'val'])
    tower_user_displacement_distance = tower_user_displacement_distance.groupBy('start').count()
    tower_user_displacement_distance = tower_user_displacement_distance.toPandas().to_dict(orient='list')
    tower_user_displacement_distance = dict(zip(tower_user_displacement_distance['start'],\
        tower_user_displacement_distance['count']))

    df = df.groupBy('start').pivot('end').agg(F.sum('value')).na.fill(value=0)

    matriz_pandas = df.toPandas()
    m = fix_json_format(json.loads(matriz_pandas.to_json(orient='index')))
    m_pandas = pd.DataFrame(m)
    m_transpose = m_pandas.transpose().to_dict()

    fd = open(TOWER_AUX_PROPERTIES_FILE, 'r')
    aux_properties = json.loads(fd.read())
    aux_properties = aux_properties['features']
    tower_aux_properties = {}
    for feature in aux_properties:
        tower = feature['properties']['id']
        pop = feature['properties']['pop']
        area = feature['properties']['area']
        tower_aux_properties[tower] = {'area': area, 'pop': pop}

    vector_data = {}
    tower_sleeper = set(sleepers.keys())
    tower_sleeper_v2 = set(sleepers_v2.keys())
    tower_worker = set(workers.keys())

    for tower in (tower_sleeper | tower_worker | tower_sleeper_v2):
        if tower not in vector_data:
            vector_data[tower] = []
        if tower in tower_sleeper:
            vector_data[tower].append(sleepers[tower])
        else:
            vector_data[tower].append(0)
        if tower in tower_sleeper_v2:
            vector_data[tower].append(sleepers_v2[tower])
        else:
            vector_data[tower].append(0)
        if tower in tower_worker:
            vector_data[tower].append(workers[tower])
        else:
            vector_data[tower].append(0)
        
        if tower in tower_user_displacement_distance and tower in sleepers:
            vector_data[tower].append(tower_user_displacement_distance[tower] / sleepers[tower])
        else:
            vector_data[tower].append(0)
        if tower in tower_aux_properties:
            area = tower_aux_properties[tower]['area']
            pop = tower_aux_properties[tower]['pop']
            vector_data[tower].append(area)
            vector_data[tower].append(pop)
        else:
            vector_data[tower].append(0)
            vector_data[tower].append(0)
    
    
    matrix = {
			'type': 'matrix',
			'labels': ['interaction'],
			'date': date,
			'matrix': m,
			'matrixTranspose': m_transpose,
	}

    vector = {
			'type': 'vector',
			'labels': ['sleeper', 'sleeper2',
			           'workers', 'mobility',
			           'area', 'pop'],
			'date': date,
			'data': vector_data
	}

    return matrix, vector


##########################################################################
############################## UserMobility ##############################
##########################################################################

get_distance_to_udf = F.udf(lambda home_tower, towers : distance_to(home_tower, towers), ArrayType(StringType()))

def radius_gyration(distance, weights):
    distance = list(map(float, distance))
    weights = list(map(float, weights))
    res = [weights[i] * distance[i] ** 2 for i in range(len(distance))]
    return math.sqrt(sum(res))

def get_radius_gyration(df):
    # get the time spent in each tower and normalize
    get_spent_time_udf = F.udf(lambda x : get_spent_time(x), ArrayType(StringType()))
    df = df.select('code', 'home_tower', 'towers', 'coords', get_spent_time_udf(F.col('acum_time')).alias('weights'))

    # calculate the distance from home tower to each tower register
    df = df.select('code', get_distance_to_udf(F.col('home_tower'), F.col('coords')).alias('distance'), 'weights')

    # calculate radius_gyration
    radius_gyration_udf = F.udf(lambda weights, distances : radius_gyration(distances, weights))
    df = df.select('code', radius_gyration_udf(F.col('weights'), F.col('distance')).alias('radius_gyration'))

    return df

def farthest_tower(towers, distances):
    distances = list(map(float, distances))
    tower = None
    max_distance = -10e8
    for i, d in enumerate(distances):
        if d > max_distance:
            max_distance = d
            tower = towers[i]
    return tower

def get_users_mobility(date, start_time='07:00', end_time='20:00', sleep_start_time='01:00', sleep_end_time='04:00'):
    spark = init_spark_session()
    
    start_time = get_time_in_seconds(start_time)
    end_time = get_time_in_seconds(end_time)
    sleep_start_time = get_time_in_seconds(sleep_start_time)
    sleep_end_time = get_time_in_seconds(sleep_end_time)
    
    df = spark.read.parquet(PARQUETS_DIR + '/' + date)
    
    # records between (start_time, end_time) OR (sleep_start_time, sleep_end_time)
    df = df.rdd.flatMap(lambda rows : between_ab_OR_dc(rows, (start_time, end_time), (sleep_start_time, sleep_end_time)))\
        .toDF(['code', 'cell_ids', 'times'])
    df = df.select('*').where(F.size(df.times) > 0)
    
    # imsi -> cell changes & amount of distinct cell
    distinct_cells_df = df.withColumn('distinct_cells', F.array_distinct(F.col('cell_ids')))\
        .select('code', 'distinct_cells', F.size(F.col('distinct_cells')).alias('distinct_cells_count'))
    
    # map cell -> tow
    df = df.rdd.flatMap(lambda x : mapp_cell_tow(x, cell_tow_mapp)).toDF(['code', 'towers', 'times'])

    # imsi -> km displacement
    km_displacement_df = df.select('code', 'towers', map_area_correlator_to_coord_udf(F.col('towers')).alias('lat_lon'))
    km_displacement_udf = F.udf(lambda coordinates : km_displacement(coordinates), StringType())
    km_displacement_df = km_displacement_df.withColumn('km_displacement', km_displacement_udf(F.col('lat_lon')))\
        .select('code', 'km_displacement')
    
    # imsi -> amount of distinct towers
    distinct_towers_df = df.withColumn('distinct_towers', F.array_distinct(F.col('towers'))).\
        select('code', 'distinct_towers', F.size(F.col('distinct_towers')).alias('distinct_towers_count')) 
    
    # process for establish sleeping zone
    users_cells_start = df.rdd.flatMap(lambda x : accumulate_count(x, sleep_start_time, sleep_end_time)).\
        toDF(['code', 'towers', 'count', 'acum_time'])

    users_cells_start = users_cells_start.select('*').where(F.size('towers') > 0)
    users_cells_start = users_cells_start.withColumn('coords', map_area_correlator_to_coord_udf(F.col('towers'))).select('*')

    # getting mean and home tower
    normalize_udf = F.udf(lambda values : [val / sum(values) for val in values], ArrayType(StringType()))
    mean_home_tower_udf = F.udf(lambda weight, coords : get_mean_home_tower(weight, coords), ArrayType(StringType()))
    
    # imsi -> spleep_area
    users_cells_start = users_cells_start.withColumn('weight', normalize_udf(F.col('count')))\
        .select('code', mean_home_tower_udf(F.col('weight'), F.col('coords')).alias('home_tower'), 
        'towers', 'coords', 'acum_time')
    
    # radius gyration
    radius_gyration_df = get_radius_gyration(users_cells_start)

    # obtain farthest tower visited
    farthest_tower_df = users_cells_start.select('code', 'towers', \
        get_distance_to_udf(F.col('home_tower'), F.col('coords')).alias('distance'))
    
    get_farthest_tower_udf = F.udf(lambda towers, distances : farthest_tower(towers, distances))
    farthest_tower_df = farthest_tower_df.select('code', \
        get_farthest_tower_udf(F.col('towers'), F.col('distance')).alias('farthest_tower'))
    
    # remove unnecessary columns
    users_cells_start = users_cells_start.select('code', 'home_tower')
    distinct_cells_df = distinct_cells_df.select('code', 'distinct_cells_count')
    distinct_towers_df = distinct_towers_df.select('code', 'distinct_towers_count')

    results_dfs = [users_cells_start, distinct_cells_df, km_displacement_df, distinct_towers_df, \
        radius_gyration_df, farthest_tower_df] 

    return users_cells_start, distinct_cells_df, km_displacement_df\
        ,distinct_towers_df, radius_gyration_df, farthest_tower_df

    '''
    user_mobility = users_cells_start.unionByName(distinct_cells_df, allowMissingColumns=True)
    for df in results_dfs[2:]:     
        user_mobility = user_mobility.unionByName(df, allowMissingColumns=True)
    return user_mobility 
    '''


###############################################
################### Testing ###################
###############################################

def test_mobility_matrix():
    '''
        time sent 1914.185616493225s = 31m
    '''
    start = time.time()
    mobility('2021-03-01s', '06:00', '10:00', '16:00', '20:00')
    end = time.time()
    print('time spent: ' + str((end - start) / 60))


def test_user_mobility():
    home_tower, dcell, km, dtow, radio, farthesttow = get_users_mobility('2021-03-01s')
    home_tower.show()
    dcell.show()
    km.show()
    dtow.show()
    radio.show()
    farthesttow.show()

if __name__ == '__main__':
    # For tests i use '2021-03-01s' parquet, smaller version of '2021-03-01' parquet day
    test_mobility_matrix()
    # test_user_mobility()
    

