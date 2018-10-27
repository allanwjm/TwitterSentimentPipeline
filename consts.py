# Fetch data from the couchDB
COUCHDB_URL = 'http://45.113.232.90/couchdbro/twitter/_design/twitter/_view/summary'
COUCHDB_USERNAME = 'readonly'
COUCHDB_PASSWORD = 'ween7ighai9gahR6'

# Shapefiles
# Download from here: http://www.abs.gov.au/AUSSTATS/abs@.nsf/DetailsPage/1270.0.55.001July%202016?OpenDocument
# Put "SA1_2016_AUST.csv", "SA1_2016_AUST.shp", ... into this folder
SHAPES_DIRECTORY = 'shapefile'

# Local storage sqlite
SHAPES_FILENAME = 'shapes.pkl.gz'
DATABASE_HOST = '127.0.0.1'
DATABASE_PORT = 3306
DATABASE_USER = 'root'
DATABASE_PASSWORD = 'toor'
DATABASE_SCHEMA = 'pipeline'

# Gccsa codes and cities
GCCSA = {
    '1GSYD': 'sydney',
    '2GMEL': 'melbourne',
    '3GBRI': 'brisbane',
    '4GADE': 'adelaide',
    '5GPER': 'perth',
    '6GHOB': 'hobart',
    '8ACTE': 'canberra'}
CITIES = list(GCCSA.values())

# Download temp cache path
TEMP_DIR = '/home/ubuntu/volume/pipelinecache'
