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

def count_occurrences(elems):
    d = {}
    for i in elems:
        if i not in d:
            d[i] = 1
        else:
            d[i] += 1
    return list(map(list, d.items()))

def origin_destination_product(start, end):
    res = []
    for cell_start, val_1 in start:
        for cell_end, val_2 in end:
            res.append([cell_start, cell_end, str(int(val_1) * int(val_2))])
    return res