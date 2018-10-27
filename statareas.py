import gzip
import pickle
from operator import itemgetter

from matplotlib.path import Path

from consts import CITIES
from consts import SHAPES_FILENAME


def load_paths():
    shapes = pickle.load(gzip.open(SHAPES_FILENAME, 'rb'))
    sa1_paths = {}
    for city in CITIES:
        paths = []
        for code, data in shapes[city]['sa1'].items():
            bounds = (data['top'], data['bottom'], data['left'], data['right'])
            for polygon in data['polygons']:
                path = Path(polygon)
                paths.append([0, code, bounds, path])
        sa1_paths[city] = paths
    return sa1_paths


def get_sa1_code(paths, city, lng, lat):
    paths = paths[city]
    for i, (count, code, bounds, path) in enumerate(paths):
        if bounds[2] <= lng <= bounds[3] and \
                bounds[1] <= lat <= bounds[0] and \
                path.contains_point((lng, lat)):
            paths[i][0] += 1
            paths.sort(key=itemgetter(0), reverse=True)
            return code
    return None


def get_sa2_code(paths, city, lng, lat):
    sa1_code = get_sa1_code(paths, city, lng, lat)
    return int(sa1_code / 100)


"""
_shapes = None
_sa1_paths = {}

def load_shapes():
    global _shapes
    if _shapes is None:
        _shapes = pickle.load(gzip.open(SHAPES_FILENAME, 'rb'))
    return _shapes


def load_sa1_paths(city):
    if city not in _sa1_paths:
        shapes = load_shapes()
        _sa1_paths[city] = []
        for code, data in shapes[city]['sa1'].items():
            bounds = (data['top'], data['bottom'], data['left'], data['right'])
            for polygon in data['polygons']:
                path = Path(polygon)
                _sa1_paths[city].append([0, code, bounds, path])
    return _sa1_paths[city]


def load_sa2_paths(city):
    if city not in _sa2_paths:
        shapes = load_shapes()
        _sa2_paths[city] = []
        for code, data in shapes[city]['sa2'].items():
            bounds = (data['top'], data['bottom'], data['left'], data['right'])
            for polygon in data['polygons']:
                path = Path(polygon)
                _sa2_paths[city].append([0, code, bounds, path])
    return _sa2_paths[city]


def get_sa2_code(city, lng, lat):
    paths = load_sa2_paths(city)
    for i, (count, code, bounds, path) in enumerate(paths):
        if bounds[2] <= lng <= bounds[3] and \
                bounds[1] <= lat <= bounds[0] and \
                path.contains_point((lng, lat)):
            paths[i][0] += 1
            paths.sort(key=itemgetter(0), reverse=True)
            return code
    return None
"""
