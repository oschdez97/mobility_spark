import json
import time
import math
import numpy as np
from os import path
import pandas as pd
import pyspark.sql.functions as F
#from utils import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, IntegerType, LongType, StructType, StructField
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

TIME_GRANULARITY = {
	'60-minutes': 60,
	'30-minutes': 30,
	'15-minutes': 15,
	'10-minutes': 10,
}

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


####################################################
################ MobilityInsideZone ################
####################################################

def get_inside_mobility(row, inside_start_time, inside_end_time, 
    outside_start_time, outside_end_time, selected_cells_ids):
    code = row[0]
    cells = row[1]
    times = row[2]
    
    r_cells = []
    r_times = []
    for i, time in enumerate(times):
        if ((inside_start_time < time < inside_end_time) and cells[i] in selected_cells_ids) and\
            not ((time < outside_start_time or time > outside_end_time) and cells[i] in selected_cells_ids):
            r_times.append(time)
            r_cells.append(cells[i])
    yield (code, r_cells, r_times)

'''

no es que el time i tiene un registro en la torre j que esta dentro de las seleccionadas

es que tiene un registro 


'''

def mobility_inside_zone(date, cells_ids, inside_start_time, inside_end_time, 
    outside_start_time, outside_end_time, time_granularity=TIME_GRANULARITY['30-minutes']):
    
    spark = init_spark_session()
    
    inside_start_time = get_time_in_seconds(inside_start_time)
    inside_end_time = get_time_in_seconds(inside_end_time)
    outside_start_time = get_time_in_seconds(outside_start_time)
    outside_end_time = get_time_in_seconds(outside_end_time)

    df = spark.read.parquet(PARQUETS_DIR + '/' + date)

    # la idea es seleccionar los que estan en el rango de horas dentro del radio
    # y los que no estan dentro del radio en el rango de horas pero que salgan 

    schema = StructType(
        [StructField('code', IntegerType(), True), 
        StructField('cells_ids', ArrayType(StringType()), True), 
        StructField('times', ArrayType(LongType()), True)]
        )

    rdd = df.rdd.flatMap(lambda row: get_inside_mobility(row, inside_start_time, inside_end_time, outside_start_time, outside_end_time, cells_ids))
    
    df = spark.createDataFrame(rdd, schema = schema)
    df = df.select('*').filter(F.size(F.col('times')) > 0)

    df.printSchema()

    return df

    # imsi IN
    # select code, towers, time where 
    # inside_start_time < times < outside_start_time
    # AND tower IN towers

    # AND

    # imsi not in
    # select code, towers, time where  times < outside_start_time OR times > outside_end_time 
    # AND tower IN towers

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

def test_mobility_inside_zone():
    date = '2021-03-01'
    cells_ids = ['176-1188', '176-1189', '176-1190', '76-3271', '76-3272', '76-3273', '76-3275', '76-3276', '76-3277', '76-8391', '76-8392', '76-8393', '176-1100', '176-1101', '176-1102', '76-10451', '76-10452', '76-10453', '76-441', '76-442', '76-443', '76-445', '76-446', '76-447', '176-6261', '176-6262', '176-6263', '72-6261', '72-6262', '72-6263', '76-11621', '76-11622', '76-11623', '76-6261', '76-6262', '76-6263', '76-6265', '76-6266', '76-6267', '176-2024', '176-2025', '176-2026', '76-491', '76-492', '76-493', '76-495', '76-496', '76-497', '76-9051', '76-9052', '76-9053', '176-1741', '176-1742', '176-1743', '76-1741', '76-1742', '76-1743', '76-1745', '76-1746', '76-1747', '76-8381', '76-8382', '76-8383', '176-511', '176-512', '176-513', '76-5131', '76-5132', '76-5133', '76-5135', '76-5136', '76-5137', '76-8801', '76-8802', '76-8803', '176-2394', '176-2395', '176-2396', '76-10031', '76-10032', '76-10033', '76-501', '76-502', '76-503', '76-505', '76-506', '76-507', '176-1771', '176-1772', '176-1773', '76-1771', '76-1772', '76-1773', '76-1775', '76-1776', '76-1777', '76-8371', '76-8372', '76-8373', '176-1751', '176-1752', '176-1753', '76-10821', '76-10825', '76-1751', '76-1752', '76-1753', '76-1755', '76-1756', '76-1757', '76-8441', '76-8442', '76-8443', '401057-1', '401057-2', '401057-3', '401069-1', '401069-2', '401069-3', '72-1196', '72-1198', '72-1199', '72-702', '72-703', '74-10501', '74-10502', '74-10503', '74-12841', '74-12845', '74-3311', '74-3312', '74-3313', '74-3315', '74-3316', '74-3317', '74-371', '74-372', '74-373', '74-375', '74-376', '74-377', '74-391', '74-392', '74-393', '176-1137', '176-1138', '176-1139', '401102-1', '401102-2', '401102-3', '76-10091', '76-10092', '76-10093', '76-1251', '76-1252', '76-1253', '76-1255', '76-1256', '76-1257', '176-1206', '176-1207', '176-1208', '176-2613', '176-2614', '176-2615', '401074-1', '401074-2', '401074-3', '401095-1', '401095-2', '401095-3', '76-10141', '76-10142', '76-10143', '76-10891', '76-10895', '76-11601', '76-11602', '76-11603', '76-11604', '76-1651', '76-1652', '76-1653', '76-1654', '76-1655', '76-1656', '76-1657', '76-1658', '76-6041', '76-6042', '76-6043', '76-6045', '76-6046', '76-6047', '176-1200', '176-1201', '176-1202', '176-563', '176-564', '176-565', '401051-1', '401051-2', '401051-3', '401070-1', '401070-2', '401070-3', '76-10111', '76-10112', '76-10113', '76-10211', '76-10212', '76-10213', '76-1121', '76-1122', '76-1123', '76-1125', '76-1126', '76-1127', '76-671', '76-672', '76-673', '76-675', '76-676', '76-677', '176-6051', '176-6052', '176-6053', '76-6051', '76-6052', '76-6053', '76-6055', '76-6056', '76-6057', '76-8431', '76-8432', '76-8433', '176-2594', '176-2595', '176-2596', '76-10461', '76-10462', '76-10463', '76-1171', '76-1172', '76-1173', '76-1175', '76-1176', '76-1177', '176-1761', '176-1762', '176-1763', '76-1761', '76-1762', '76-1763', '76-1765', '76-1766', '76-1767', '76-8421', '76-8422', '76-8423', '176-2140', '176-2141', '176-2142', '176-2628', '176-2629', '176-2630', '76-1701', '76-1702', '76-1703', '76-1705', '76-1706', '76-1707', '76-3331', '76-3332', '76-3333', '76-3335', '76-3336', '76-3337', '76-8321', '76-8322', '76-8323', '76-9061', '76-9062', '76-9063', '176-2604', '176-2605', '176-2606', '76-10471', '76-10472', '76-10473', '76-10841', '76-10845', '76-3291', '76-3292', '76-3293', '76-3295', '76-3296', '76-3297', '176-6101', '176-6102', '176-6103', '76-10871', '76-10875', '76-11091', '76-11092', '76-11093', '76-11095', '76-11096', '76-11097', '76-11101', '76-11102', '76-11103', '176-2616', '176-2617', '176-2618', '76-13571', '76-13572', '76-13573', '76-13575', '76-13576', '76-13577', '76-13581', '76-13582', '76-13583', '176-1088', '176-1089', '176-1090', '401060-1', '401060-2', '401060-3', '76-12601', '76-12602', '76-12603', '76-3351', '76-3352', '76-3353', '76-3355', '76-3356', '76-3357', '176-2506', '176-2507', '176-2508', '401083-1', '401083-2', '401083-3', '76-10171', '76-10172', '76-10173', '76-6021', '76-6022', '76-6023', '76-6025', '76-6026', '76-6027', '176-1221', '176-1222', '176-1223', '176-2169', '176-2170', '176-2171', '401064-1', '401064-2', '401064-3', '401114-1', '401114-2', '401114-3', '76-1011', '76-1012', '76-1013', '76-10131', '76-10132', '76-10133', '76-1015', '76-1016', '76-1017', '76-10511', '76-10512', '76-10513', '76-6011', '76-6012', '76-6013', '76-6015', '76-6016', '76-6017', '176-2348', '176-2349', '176-2350', '401085-1', '401085-2', '401085-3', '76-10101', '76-10102', '76-10103', '76-3321', '76-3322', '76-3323', '76-3325', '76-3326', '76-3327', '74-10831', '74-10835', '401063-1', '401063-2', '401063-3', '72-2610', '72-2611', '72-2612', '74-1661', '74-1662', '74-1663', '74-1665', '74-1666', '74-1667', '74-8241', '74-8242', '74-8243', '176-1179', '176-1180', '176-1181', '401100-1', '401100-2', '401100-3', '76-10491', '76-10492', '76-10493', '76-10494', '76-1081', '76-1082', '76-1083', '76-1084', '76-1085', '76-10851', '76-10855', '76-1086', '76-1087', '76-1088', '401081-1', '401081-2', '401081-3', '72-2274', '72-2275', '72-2276', '72-2600', '72-2601', '72-2602', '74-9081', '74-9082', '74-9083', '74-941', '74-942', '74-943', '74-945', '74-946', '74-947', '176-10421', '176-10422', '176-10423', '176-2625', '176-2626', '176-2627', '401112-1', '401112-2', '401112-3', '401192-1', '401192-2', '401192-3', '76-10421', '76-10422', '76-10423', '76-10425', '76-10426', '76-10427', '76-10431', '76-10432', '76-10433', '76-2691', '76-2692', '76-2693', '76-2695', '76-2696', '76-2697', '76-2701', '76-2702', '76-2703', '176-2667', '176-2668', '176-2669', '401115-1', '401115-2', '401115-3', '76-10861', '76-10862', '76-10865', '76-10866', '76-13591', '76-13592', '76-13593', '76-13595', '76-13596', '76-13597', '76-13601', '76-13602', '76-13603', '176-261', '176-262', '176-263', '76-1131', '76-1132', '76-1133', '76-1135', '76-1136', '76-1137', '76-11561', '76-11562', '76-11563', '176-9021', '176-9022', '176-9023', '76-1161', '76-1162', '76-1163', '76-1165', '76-1166', '76-1167', '76-8331', '76-8332', '76-8333', '176-2335', '176-2336', '176-2337', '76-10151', '76-10152', '76-10153', '76-12911', '76-12915', '76-1851', '76-1852', '76-1853', '76-1855', '76-1856', '76-1857', '176-172', '176-173', '176-174', '401082-1', '401082-2', '401082-3', '76-10121', '76-10122', '76-10123', '76-3341', '76-3342', '76-3343', '76-3345', '76-3346', '76-3347', '176-1231', '176-1232', '176-1233', '401113-1', '401113-2', '401113-3', '76-10521', '76-10522', '76-10523', '76-1871', '76-1872', '76-1873', '76-1875', '76-1876', '76-1877', '76-11021', '76-11025', '176-2043', '176-2044', '176-2045', '401076-1', '401076-2', '401076-3', '76-10161', '76-10162', '76-10163', '76-6001', '76-6002', '76-6003', '76-6005', '76-6006', '76-6007', '76-12901', '76-12905', '107-11021', '107-11022', '107-11023', '107-211', '107-212', '107-213', '107-215', '107-216', '107-217', '7-995', '7-996', '7-997', '107-101', '107-102', '107-10251', '107-10252', '107-10253', '107-103', '107-105', '107-106', '107-107', '401077-1', '401077-2', '401077-3', '7-2452', '7-2453', '7-2454', '107-561', '107-562', '107-563', '107-565', '107-566', '107-567', '107-8221', '107-8222', '107-8223', '401098-1', '401098-2', '401098-3', '7-1022', '7-1023', '7-1024']
    
    inside_start_time = '10:00'
    inside_end_time = '14:00' 
    outside_start_time = '00:10'
    outside_end_time = '04:50'
    
    res = mobility_inside_zone(date, cells_ids, inside_start_time, inside_end_time, outside_start_time, outside_end_time)
    res.show()

if __name__ == '__main__':
    # For tests i use '2021-03-01s' parquet, smaller version of '2021-03-01' parquet day
    # test_mobility_matrix()
    # test_user_mobility()
    test_mobility_inside_zone()


'''
((inside_start_time < time < inside_end_time) and cells[i] in selected_cells_ids) 
and
((time < outside_start_time or time > outside_end_time) and cells[i] in selected_cells_ids)
'''