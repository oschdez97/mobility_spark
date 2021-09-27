import numpy as np
from collections import Counter
from scipy.spatial import KDTree
from math import sqrt, pi, sin, cos, acos, atan2

earth_radius = 6371

def get_time_in_seconds(time):
    '''
        :param: time str : '16:30'
        :return int: Number of seconds 59400
    '''
    h, m = list(map(int, time.split(':')))
    return h * 3600 + m * 60

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

# generator
def get_range(rows, time_init, time_end):
    """
        Get the range of values between a range
        :param list rows: The list of imsis, towers and times
        :param int time_init: The low end of the range
        :param int time_end: The high end of the range
        :return: The list of imsis, towers and times inside the range of times
    """
    code = rows[0]
    towers = rows[1]
    times = rows[2]
    low  = lower_bound(times, time_init)
    high = upper_bound(times, time_end)
    yield (code, towers[low:high], times[low:high])

# indices of the interval
def get_range_interval(times, time_init, time_end):
    low  = lower_bound(times, time_init)
    high = upper_bound(times, time_end)
    return [low, high]

def mapp_cell_tow(rows, mapp):
    """
        Map from cell to tower(area_correlator)
        :param list rows: The list of imsis, cells and times        
        :param mapp dict: The dictionary cell -> tow
        :return: The list of imsis, towers and times 
    """
    cells  = []
    times  = []
    zipped = list(zip(rows[1], rows[2]))
    for _id, _time in zipped:
        if mapp.__contains__(_id):
            cells.append(mapp[_id])
            times.append(_time)
    yield (rows[0], cells, times)

def count_occurrences_and_normalize(elems):
    count = dict(Counter(elems))
    normalize = sum(count.values())
    for i in count:
        count[i] /= normalize
        count[i] = round(count[i], 4)
    return list(map(list, count.items()))


def flat_origin_destination_product(row):
    for cell_start, val_1 in row[0][0]:
        for cell_end, val_2 in row[0][1]:
            yield (cell_start, cell_end, float(val_1) * float(val_2))


def distance_displacement(row, mapp):
    tower_start = row[0]
    tower_end = row[1]
    product = row[2]

    tower_start_coords = mapp[tower_start]
    tower_end_coords = mapp[tower_end]
    distance = distance_in_km_between_coordinates(tower_start_coords, tower_end_coords)
    
    yield (row[0], distance * product)


def fix_json_format(jobj):
    m = {}
    for i in jobj:
        m[jobj[i]['start']] = jobj[i]
        del m[jobj[i]['start']]['start']
    return m


def accumulate_count(data, lower_bound, upper_bound):
    code = data[0]
    towers = data[1]
    times = data[2]
    t_res = {}

    for i, tow in enumerate(towers):
        if lower_bound < times[i] < upper_bound:
            if tow not in t_res:
                t_res[tow] = [1, times[i]]
            else:
                t_res[tow][0] += 1
                t_res[tow][1] += times[i]

    count_towers = []
    sum_of_time_by_tower = []    
    for i in list(t_res.values()):
        count_towers.append(i[0])
        sum_of_time_by_tower.append(i[1])

    yield (code, list(t_res.keys()), count_towers, sum_of_time_by_tower)

def map_area_correlator_to_coord(towers, mapp):
    res = []
    for i in towers:
        if mapp.__contains__(i):
            res.append(mapp[i])
    return res

def map_coord_to_area_correlator(coord, mapp):
    coord = str(coord)
    if mapp.__contains__(coord):
        return mapp[coord]

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

def degrees_to_radians(degrees):
	return degrees * pi / 180

def distance_in_km_between_coordinates(coord1, coord2):
	lat1, lon1 = list(map(float, coord1))
	lat2, lon2 = list(map(float, coord2))
    
	dLat = degrees_to_radians(lat2 - lat1)
	dLon = degrees_to_radians(lon2 - lon1)

	lat1 = degrees_to_radians(lat1)
	lat2 = degrees_to_radians(lat2)

	a = sin(dLat / 2) ** 2 + sin(dLon / 2) ** 2 * cos(lat1) * cos(lat2)
	c = 2 * atan2(sqrt(a), sqrt(1 - a))

	return earth_radius * c

def km_displacement(elems):
    res = []
    for i in range(len(elems)-1):
        d = distance_in_km_between_coordinates(elems[i], elems[i+1])
        res.append(d)
    return sum(res)

def between_ab_OR_dc(data, interval_1, interval_2):
    code   = data[0]
    towers = data[1]
    times  = data[2]
    interval_1 = get_range_interval(times, interval_1[0], interval_1[1])
    interval_2 = get_range_interval(times, interval_2[0], interval_2[1])
    zip_data = list(zip(times, towers))
    a = zip_data[interval_1[0] : interval_1[0] + interval_1[1]]
    b = zip_data[interval_2[0] : interval_2[0] + interval_2[1]]
    res = list(set(a) | set(b))
    res.sort()
    towers = [x[1] for x in res]
    times  = [x[0] for x in res]
    yield (code, towers, times)

def get_spent_time(times):
    last_time = 24 * 60 * 60
    res = [0] * len(times)
    for i in range(0, len(times) - 1):
        current_register = times[i]
        next_register = times[i + 1]
        delta = abs(next_register - current_register)
        half_expended_time = delta / 2
        res[i] += half_expended_time
        res[i+1] += half_expended_time
        #res[i] = delta
    res[-1] += abs(last_time - times[-1])

    # normalize
    normalized = [val / last_time for val in res]
    return normalized

def distance_to(home_tower, towers):
    res = []
    for tower in towers:
        res.append(distance_in_km_between_coordinates(home_tower, tower)) 
    return res