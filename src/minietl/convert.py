from . import hints
from types import SimpleNamespace
from typing import Iterable

def tabular_to_dict(headers):
    def inner(row):
        zipped = zip(headers, row)
        return {k: v for k, v in zipped}

    return hints.jcallable(inner)


@hints.jsplitter
def header_tabular_to_dict(iterable):
    headers = next(iterable)
    func = tabular_to_dict(headers).job

    for item in iterable:
        yield func(item)


@hints.jcallable
def dict_to_object(item):
    return SimpleNamespace(**item)


@hints.jsplitter
def dict_to_tabular(iterable):
    items = list(iterable)

    headers = {}
    for item in items:
        for k in item.keys():
            headers.add(k)

    header_row = list(headers)
    yield header_row
    for item in items:
        yield [item.get(h) for h in header_row]
