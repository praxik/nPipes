# An assortment of handy FP stuff that is missing from Python

import concurrent.futures

def identity(x):
    return x


def concurrentMap(f, xs):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        return executor.map(f, xs)
