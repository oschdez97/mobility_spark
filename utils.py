import numpy as np
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
	lat1, lon1, lat2, lon2 = coord1[0], coord1[1], coord2[0], coord2[1]

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

def mapp_tow_cell(elem, mapp):
    err    = []
    cells  = []
    times  = []
    zipped = list(zip(elem[1], elem[2]))
    for _id, _time in zipped:
        if _id not in mapp or mapp[_id] == None or mapp[_id] == '':
           err.append((_id, _time))
        else:
            cells.append(mapp[_id])
            times.append(_time)
    yield (elem[0], cells, times)
    
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