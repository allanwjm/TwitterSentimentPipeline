import csv
import gzip
import operator
import os
import pickle

import shapefile

from consts import CITIES
from consts import GCCSA
from consts import SHAPES_DIRECTORY
from consts import SHAPES_FILENAME

base_path = os.path.split(os.path.realpath(__file__))[0]

output_data = {
    city: {
        'sa2': {
            # sa2_code <int>:  {
            # sa2_name: <str>,
            # area (sqkm): <float>,
            # top (lat): <float>,
            # bottom (lat): <float>,
            # left (lng): <float>,
            # right (lng): <float>,
            # polygons: [
            #   [(lng, lat), ...], ...
            # ]}
        },
        'sa1': {
            # sa1_code <int>:  {
            # area (sqkm): <float>,
            # top (lat): <float>,
            # bottom (lat): <float>,
            # left (lng): <float>,
            # right (lng): <float>,
            # polygons: [
            #   [(lng, lat), ...], ...
            # ]}
        },
        'bounds': {
            'top': -999,
            'bottom': 999,
            'left': 999,
            'right': -999,
        }
    } for city in CITIES
}

# Load files
print('Loading SA2 files...')
sa2_csv = open(os.path.join(base_path, SHAPES_DIRECTORY, 'SA2_2016_AUST.csv'), 'r')
sa2_reader = csv.DictReader(sa2_csv)
sa2_shp = shapefile.Reader(os.path.join(base_path, SHAPES_DIRECTORY, 'SA2_2016_AUST'))
sa2_shapes = sa2_shp.shapes()

print('Loading SA1 files...')
sa1_csv = open(os.path.join(base_path, SHAPES_DIRECTORY, 'SA1_2016_AUST.csv'), 'r')
sa1_reader = csv.DictReader(sa1_csv)
sa1_shp = shapefile.Reader(os.path.join(base_path, SHAPES_DIRECTORY, 'SA1_2016_AUST'))
sa1_shapes = sa1_shp.shapes()

# Process SA2
print('Processing SA2...')
for info, shape in zip(sa2_reader, sa2_shapes):
    sa2_code = int(info['SA2_MAINCODE_2016'])
    sa2_name = info['SA2_NAME_2016']
    area = info['AREA_ALBERS_SQKM']
    gccsa_code = info['GCCSA_CODE_2016']

    if gccsa_code not in GCCSA.keys():
        continue
    else:
        city = GCCSA[gccsa_code]

    points = shape.points
    if not points:
        continue
    else:
        # Get the boundary of this sa2
        top = max(map(operator.itemgetter(1), points))
        bottom = min(map(operator.itemgetter(1), points))
        left = min(map(operator.itemgetter(0), points))
        right = max(map(operator.itemgetter(0), points))

    if output_data[city]['bounds']['top'] < top:
        output_data[city]['bounds']['top'] = top
    if output_data[city]['bounds']['bottom'] > bottom:
        output_data[city]['bounds']['bottom'] = bottom
    if output_data[city]['bounds']['left'] > left:
        output_data[city]['bounds']['left'] = left
    if output_data[city]['bounds']['right'] < right:
        output_data[city]['bounds']['right'] = right

    polygons = [[]]
    polygon = polygons[0]
    point_set = set()
    for p in points:
        # polygon.append({'lng': p[0], 'lat': p[1]})
        polygon.append(p)
        if p in point_set:
            polygons.append([])
            polygon = polygons[-1]
            point_set = set()
        else:
            point_set.add(p)

    polygons = filter(bool, polygons)

    output_data[city]['sa2'][sa2_code] = {
        'sa2_name': sa2_name,
        'area': area,
        'top': top, 'bottom': bottom,
        'left': left, 'right': right,
        'polygons': polygons}

# Process SA1
print('Processing SA1...')
for info, shape in zip(sa1_reader, sa1_shapes):
    sa1_code = int(info['SA1_MAINCODE_2016'])
    gccsa_code = info['GCCSA_CODE_2016']

    if gccsa_code not in GCCSA.keys():
        continue
    else:
        city = GCCSA[gccsa_code]

    points = shape.points
    if not points:
        continue
    else:
        top = max(map(operator.itemgetter(1), points))
        bottom = min(map(operator.itemgetter(1), points))
        left = min(map(operator.itemgetter(0), points))
        right = max(map(operator.itemgetter(0), points))

    polygons = [[]]
    polygon = polygons[0]
    point_set = set()
    for p in points:
        # polygon.append({'lng': p[0], 'lat': p[1]})
        polygon.append(p)
        if p in point_set:
            polygons.append([])
            polygon = polygons[-1]
            point_set = set()
        else:
            point_set.add(p)

    polygons = filter(bool, polygons)

    output_data[city]['sa1'][sa1_code] = {
        'area': info['AREA_ALBERS_SQKM'],
        'top': top, 'bottom': bottom,
        'left': left, 'right': right,
        'polygons': polygons}

# Output
print('Saving as file...')
pkl_file = gzip.open(os.path.join(base_path, SHAPES_FILENAME), 'wb')
pickle.dump(output_data, pkl_file)

print('Finish!')
