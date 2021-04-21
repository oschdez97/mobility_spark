import random
import string

def generate(n=1000):
    data = []
    for i in range(n):
        code = random.randint(0, 1000)
        cells = []
        times = []        
        interactions = random.randint(0, 100)
        for _ in range(interactions):
            cell = ''.join(random.choice(string.ascii_lowercase) for j in range(5))
            time = random.randint(0, 1500)
            cells.append(cell)
            times.append(time)
        times.sort()
        data.append([code, cells, times])
    return data

if __name__ == '__main__':
    result = generate()
    print(result[0])