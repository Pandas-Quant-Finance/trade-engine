from functools import reduce


def coalesce(*arg):
    return reduce(lambda x, y: x if x is not None else y, arg)


def is_empty_iterable(it):
    return it is None or len(it) <= 0