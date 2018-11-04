import csv
import io
import sys
from datetime import datetime
from multiprocessing.pool import ThreadPool
from operator import itemgetter

import click
from flask import Flask
from flask import abort
from flask import make_response
from flask import request

from consts import CITIES
from database import connect
from statareas import get_sa2_name
from statareas import load_shapes

shapes = None
app = Flask(__name__)


def get_arg(key, default=None, type=str):
    return request.args.get(key, default, type)


def sql_worker(sql):
    conn = connect()
    c = conn.cursor()
    c.execute(sql)
    result = {}
    for key, value in c.fetchall():
        result[key] = value
    return result


@app.route('/csv/<city>', methods=['GET'])
def output_csv(city):
    global shapes
    if city not in CITIES:
        abort(400)

    table_name = '%s_data' % city
    queries = {
        'twitter_count':
            'SELECT sa2_code, COUNT(*) FROM %s GROUP BY sa2_code;' % table_name,
        'user_count':
            'SELECT sa2_code, COUNT(DISTINCT user_id) FROM %s GROUP BY sa2_code;' % table_name,
        'avg_score':
            'SELECT sa2_code, AVG(compound) FROM %s GROUP BY sa2_code;' % table_name,
        'pos_count':
            'SELECT sa2_code, COUNT(*) FROM %s WHERE compound >= 0.05 GROUP BY sa2_code;' % table_name,
        'neg_count':
            'SELECT sa2_code, COUNT(*) FROM %s WHERE compound <= -0.05 GROUP BY sa2_code;' % table_name}
    queries = list(queries.items())

    keys = list(map(itemgetter(0), queries))
    sqls = list(map(itemgetter(1), queries))
    pool = ThreadPool()
    results = pool.map(sql_worker, sqls)

    if shapes is None:
        shapes = load_shapes()

    data = {}
    for sa2, info in shapes[city]['sa2'].items():
        data[sa2] = {
            'sa2_code': sa2,
            'sa2_name': get_sa2_name(shapes, city, sa2)}
        for key in keys:
            data[sa2][key] = 0

    for key, result in zip(keys, results):
        for sa2, value in result.items():
            if sa2 not in data:
                continue
            data[sa2][key] = value

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, [
        'sa2_code', 'sa2_name', 'twitter_count', 'user_count', 'avg_score', 'pos_count', 'neu_count', 'neg_count'])
    writer.writeheader()

    for rowdict in data.values():
        rowdict['neu_count'] = rowdict['twitter_count'] - rowdict['pos_count'] - rowdict['neg_count']
        writer.writerow(rowdict)

    buffer.flush()
    buffer.seek(0)
    content = buffer.read()

    # Return as CSV file
    response = make_response(content)
    response.headers["Content-Disposition"] = "attachment; filename=%s.csv" % city
    return response


@click.command()
@click.option('--port', '-p',
              type=int, default=5000,
              help='RESTful API port')
@click.option('--quiet', '-q',
              is_flag=True, default=False,
              help='Disable all stdout outputs (redirected to log-api.txt)')
def run_api(port, quiet):
    if quiet:
        logfp = open('log-api.txt', 'a')
        sys.stdout = logfp
        sys.stderr = logfp
    print('-' * 40)
    print('API process started at: %s' % datetime.now().replace(microsecond=0))
    app.run(host='0.0.0.0', port=port, debug=False)


if __name__ == '__main__':
    run_api()
