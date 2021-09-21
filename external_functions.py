import numpy as np
from collections import Counter

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

def mapp_tow_cell(rows, mapp):
    """
        Map from tower to cell
        :param list rows: The list of imsis, towers and times        
        :param mapp dict: The dictionary tow -> cell
        :return: The list of imsis, cells and times 
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


def fix_json_format(jobj):
    m = {}
    for i in jobj:
        m[jobj[i]['start']] = jobj[i]
        del m[jobj[i]['start']]['start']
    return m