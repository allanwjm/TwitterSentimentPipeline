import sys
from datetime import datetime

import click
from flask import Flask
from flask import abort
from flask import make_response
from flask import request

from consts import CITIES

app = Flask(__name__)


def get_arg(key, default=None, type=str):
    return request.args.get(key, default, type)


def as_csv(content, filename):
    response = make_response(content)
    response.headers["Content-Disposition"] = "attachment; filename=%s" % filename
    return response


def csv_worker(city):
    pass


@app.route('/<city>', methods=['GET'])
def output_csv(city):
    if city not in CITIES:
        abort(404)
    return as_csv()


@click.option('--port', '-p',
              type=int, default=5000,
              help='RESTful API port')
def run_api(port):
    logfp = open('log-api.txt', 'a')
    sys.stdout = logfp
    sys.stderr = logfp
    print('-' * 40)
    print('API process started at: %s' % datetime.now().replace(microsecond=0))
    app.run(host='0.0.0.0', port=port, debug=False)


if __name__ == '__main__':
    run_api()
