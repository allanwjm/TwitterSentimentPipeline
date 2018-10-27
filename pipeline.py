import os
import sys
import ujson
import warnings
from datetime import datetime
from datetime import timedelta
from multiprocessing import Process
from multiprocessing import Queue
from multiprocessing import cpu_count
from multiprocessing import freeze_support
from multiprocessing.pool import ThreadPool
from time import sleep

import click

from consts import CITIES
from consts import TEMP_DIR
from database import connect
from database import get_metadata
from database import initialize
from database import insertmany
from database import set_metadata
from fetcher import fetch_as_file
from sentiment import get_sentiment_scores
from statareas import get_sa1_code
from statareas import load_paths


def capitalize(s):
    if s == '':
        return ''
    return s[0].upper() + s[1:]


def process_doc(city, doc, areaq):
    if doc['coordinates'] is None:
        return None

    lng, lat = doc['coordinates']['coordinates']
    areaq.put((city, lng, lat))
    sa1_code = areaq.get()
    if sa1_code is None:
        return None

    sa2_code = int(sa1_code / 100)

    tid = int(doc['id_str'])
    user_id = int(doc['user']['id_str'])
    text = doc['text']
    created_at = datetime.strptime(doc['created_at'], '%a %b %d %H:%M:%S %z %Y')
    created_at = created_at.replace(microsecond=0)
    year = created_at.year

    lang = doc['lang']
    if lang is None or len(lang) > 2:
        lang = '--'

    if lang == 'en':
        scores = get_sentiment_scores(text)
        pos = scores['pos']
        neu = scores['neu']
        neg = scores['neg']
        compound = scores['compound']
    else:
        pos = None
        neu = None
        neg = None
        compound = None

    return (tid, user_id, text,
            created_at, year, lang,
            lng, lat, sa1_code, sa2_code,
            pos, neu, neg, compound)


def pipeline_worker(i, taskq, logq, areaq):
    warnings.filterwarnings('ignore', category=Warning)
    conn = connect()
    tmpfile = None
    tmpfp = None
    try:
        # Worker main loop
        while True:
            city, fetch_start = taskq.get()
            fetch_end = fetch_start + timedelta(days=1)
            fetch_count = 0
            insert_count = 0
            tmpfile = os.path.join(TEMP_DIR, '%s-%s.json' % (city, fetch_start.date()))
            fetch_as_file(city, fetch_start, fetch_end, tmpfile)
            tmpfp = open(tmpfile, 'r', encoding='utf-8')
            data_list = []
            for row in tmpfp:
                try:
                    row = row.strip().rstrip(',')
                    doc = ujson.loads(row)['doc']
                    fetch_count += 1
                    tweet = process_doc(city, doc, areaq)
                    if tweet is not None:
                        data_list.append(tweet)
                except ValueError:
                    continue

            if data_list:
                insert_count += insertmany(conn, city, data_list)
            set_metadata(conn, '%s_fetch_end' % city, fetch_end)
            logq.put('%2d: %-9s %s: fetched: %d, inserted: %d' % (
                i, capitalize(city), fetch_start.date(), fetch_count, insert_count))
            tmpfp.close()
            os.unlink(tmpfile)

    except KeyboardInterrupt:
        pass

    finally:
        conn.close()
        if tmpfp is not None and not tmpfp.closed:
            tmpfp.close()
            os.unlink(tmpfile)


def dispatch_worker(taskq):
    conn = connect()
    fetch_city_ends = {city: get_metadata(conn, '%s_fetch_end' % city) for city in CITIES}
    fetch_start = min(fetch_city_ends.values()) - timedelta(days=1)
    try:
        while True:
            fetch_start = fetch_start + timedelta(days=1)
            fetch_end = fetch_start + timedelta(days=1)

            while fetch_start > datetime.now() - timedelta(days=7):
                sleep(60 * 60)

            for city in CITIES:
                if fetch_city_ends[city] < fetch_end:
                    taskq.put((city, fetch_start))

    except KeyboardInterrupt:
        pass

    finally:
        conn.close()


def sa_resolve_thread(arg):
    paths, areaq = arg
    try:
        while True:
            city, lng, lat = areaq.get()
            sa1_code = get_sa1_code(paths, city, lng, lat)
            areaq.put(sa1_code)
    except KeyboardInterrupt:
        pass


def sa_resolve_worker(areaqs):
    try:
        paths = load_paths()
        pool = ThreadPool(len(areaqs))
        pool.map(sa_resolve_thread, [(paths, areaq) for areaq in areaqs])
        pool.terminate()
        pool.join()
    except KeyboardInterrupt:
        pass


def run_pipeline(number_workers):
    logfp = open('log-pipeline.txt', 'a')
    logq = Queue()
    taskq = Queue(maxsize=100)

    if not os.path.isdir(TEMP_DIR):
        os.mkdir(TEMP_DIR)

    def log(s):
        s = '[%s] %s' % (datetime.now().replace(microsecond=0), s)
        print(s)
        sys.stdout.flush()
        logfp.write(s + '\n')
        logfp.flush()

    log('-' * 40)
    log('Starting worker processes...')

    dispatcher = Process(target=dispatch_worker, args=(taskq,))
    dispatcher.start()

    if number_workers is None:
        number_workers = cpu_count()
    areaqs = [Queue() for _ in range(number_workers)]
    workers = [Process(target=pipeline_worker, args=(i, taskq, logq, areaqs[i])) for i in range(number_workers)]
    for worker in workers:
        worker.start()

    sa_resolver = Process(target=sa_resolve_worker, args=(areaqs,))
    sa_resolver.start()

    log('%d workers started.' % number_workers)

    try:
        # Main thread: do logging
        while True:
            log(logq.get())

    except KeyboardInterrupt:
        log('Keyboard interrupted! Exiting program...')

    dispatcher.terminate()
    dispatcher.join()
    for worker in workers:
        worker.terminate()
        worker.join()
    sa_resolver.terminate()
    sa_resolver.join()

    log('Program exited!')
    logfp.close()


@click.command()
@click.option('--init-database', '-i',
              is_flag=True, default=False,
              help='Initialize and clear the database')
@click.option('--number-workers', '-n',
              type=int, default=None,
              help='Number of workers')
def main(init_database, number_workers):
    if init_database:
        if click.confirm('Are you sure to initialize the database? This will clear the database if exists!'):
            initialize()
        else:
            print('Aborted.')
    else:
        run_pipeline(number_workers)


if __name__ == '__main__':
    freeze_support()
    main()
