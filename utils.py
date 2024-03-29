import numpy as np
from scipy.spatial import KDTree
from math import sqrt, pi, sin, cos, acos, atan2
from datetime import datetime, timedelta, date
earth_radius = 6371


def day_of_year(date_str):
	fmt_str = r"%Y-%m-%d"
	date_obj = datetime.strptime(date_str, fmt_str)
	first_date = datetime(date_obj.year, 1, 1)
	delta = date_obj - first_date
	return (delta.days+1) % 92


def degrees_to_radians(degrees):
	return degrees * pi / 180


def distance_in_km_between_coordinates(coord1, coord2):
	# TODO: hacer esto mas elegante!!
    #lat1, lon1, lat2, lon2 = float(coord1[0]), float(coord1[1]), float(coord2[0]), float(coord2[1])
    
	lat1, lon1 = list(map(float, coord1))
	lat2, lon2 = list(map(float, coord2))
    
	dLat = degrees_to_radians(lat2 - lat1)
	dLon = degrees_to_radians(lon2 - lon1)

	lat1 = degrees_to_radians(lat1)
	lat2 = degrees_to_radians(lat2)

	a = sin(dLat / 2) ** 2 + sin(dLon / 2) ** 2 * cos(lat1) * cos(lat2)
	c = 2 * atan2(sqrt(a), sqrt(1 - a))

	return earth_radius * c


def get_tuple_str(list):
	return str(list).replace('[', '(').replace(']', ')')


def round_time(time, interval):
	new_minute = (time.minute//interval)*interval
	new_time = datetime(1,1,1, time.hour, new_minute).time()
	return str(new_time)


def get_datetime_by_time(time):
	return datetime.combine(date.today(), time)

def lower_bound(elems, n):
    low  = 0
    high = len(elems)
    while(low < high):
        mid = int(low + (high - low) / 2)
        if n <= elems[mid]:
            high = mid
        else:
            low = mid + 1
    return low

def upper_bound(elems, n):
    low = 0
    high = len(elems)
    while(low < high):
        mid = int(low + (high - low) / 2)
        if n >= elems[mid]:
            low = mid + 1
        else:
            high = mid
    return low

# Return [low, length], starting at index 1, pyspark.sql.functions.slice work like that...
def get_range(times, time_init, time_end):
    low  = lower_bound(times, time_init)
    high = upper_bound(times, time_end)
    return [low+1, high - low]

def mapp_tow_cell(elem, mapp):
    cells  = []
    times  = []
    zipped = list(zip(elem[1], elem[2]))
    for _id, _time in zipped:
        if mapp.__contains__(_id):
            cells.append(mapp[_id])
            times.append(_time)
    yield (elem[0], cells, times)

def mapp_cell_latlon(row, mapp):
    lats_lons = []
    cells = row[1]
    for cell in cells:
        if mapp.__contains__(cell):
            lats_lons.append(mapp[cell])
    yield (row[0], cells, lats_lons)
            
# UserMobility func getting broadcast error if i put it inside class definition
def between_ab_OR_dc(data, interval_1, interval_2):
    code   = data[0]
    towers = data[1]
    times  = data[2]
    interval_1 = get_range(times, interval_1[0], interval_1[1])
    interval_2 = get_range(times, interval_2[0], interval_2[1])
    zip_data = list(zip(times, towers))
    a = zip_data[interval_1[0] - 1 : interval_1[0] + interval_1[1] - 1]
    b = zip_data[interval_2[0] - 1 : interval_2[0] + interval_2[1] - 1]
    res = list(set(a) | set(b))
    res.sort()
    towers = [x[1] for x in res]
    times  = [x[0] for x in res]
    yield (code, towers, times)

# UserMobility func getting broadcast error if i put it inside class definition 
def accumulate_count(data, lower_bound, upper_bound):
    code = data[0]
    towers = data[1]
    times = data[2]
    res = {} 
    for i, tow in enumerate(towers):
        if lower_bound < times[i] < upper_bound:
            if tow not in res:
                res[tow] = 1
            else:
                res[tow] += 1
    yield (code, list(res.keys()), list(res.values()))

# MobilityMatrix        
def count_occurrences_and_normalize(elems):
    d = {}
    for i in elems:
        if i not in d:
            d[i] = 1
        else:
            d[i] += 1
    normalize = float(np.sum(np.array([count for count in d.values()])))
    for i in d:
        d[i] /= normalize
        d[i] = round(d[i], 4)
    return list(map(list, d.items()))

def flat_origin_destination_product(row):
    for cell_start, val_1 in row[0][0]:
        for cell_end, val_2 in row[0][1]:
            yield (cell_start, cell_end, float(val_1) * float(val_2))
            
            
def km_displacement(elems):
    res = []
    for i in range(len(elems)-1):
        d = distance_in_km_between_coordinates(elems[i], elems[i+1])
        res.append(d)
    return sum(res)

# UserMobility    
def map_area_correlator_to_coord(towers, mapp):
    res = []
    for i in towers:
        res.append(mapp[i])
    return res
    
def get_mean_home_tower(weights, coords):
    mean_tower = [0, 0]
    weights = list(map(float, weights))
    coords = [list(map(float, coord)) for coord in coords]
    for i in range(len(coords)):
        # mean_tower = mean_tower + weights[i] * tower_coord
        # getting trouble with numpy and udf
        tower_coord = coords[i]
        tmp = [weights[i] * tower_coord[0], weights[i] * tower_coord[1]]
        mean_tower = [mean_tower[0] + tmp[0], mean_tower[1] + tmp[1]]
    kdtree = KDTree(coords)
    kdtree_query = kdtree.query(mean_tower, 1)
    res = list(map(str, list(kdtree.data[kdtree_query[1]])))
    return res