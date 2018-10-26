import sys
import warnings
from datetime import datetime
from datetime import timedelta
from multiprocessing import Process
from multiprocessing import Queue
from multiprocessing import freeze_support
from time import sleep

import click

from consts import CITIES
from database import connect
from database import get_metadata
from database import initialize
from database import insertmany
from database import set_metadata
from fetcher import fetch
from sentiment import get_sentiment_scores
from statareas import get_sa1_code


def capitalize(s):
    if s == '':
        return ''
    return s[0].upper() + s[1:]


def process_data_row(city, row):
    t = row['doc']
    if t['coordinates'] is None:
        return None

    lng, lat = t['coordinates']['coordinates']
    sa1_code = get_sa1_code(city, lng, lat)
    if sa1_code is None:
        return None

    sa2_code = int(sa1_code / 100)

    tid = int(t['id_str'])
    user_id = int(t['user']['id_str'])
    text = t['text']
    created_at = datetime.strptime(t['created_at'], '%a %b %d %H:%M:%S %z %Y')
    year = created_at.year

    lang = t['lang']
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


def pipeline_worker(city, logq):
    conn = connect()
    fetch_start = get_metadata(conn, '%s_fetch_end' % city) - timedelta(days=1)

    try:
        # Worker main loop
        logq.put('Worker started: %s' % city)
        while True:
            fetch_start = fetch_start + timedelta(days=1)
            fetch_end = fetch_start + timedelta(days=1)
            while fetch_start > datetime.now() - timedelta(days=7):
                sleep(60 * 60)

            fetch_count = 0
            insert_count = 0
            fetch_data = fetch(city, fetch_start, fetch_end)
            data_list = []
            for row in fetch_data['rows']:
                fetch_count += 1
                tweet = process_data_row(city, row)
                if tweet is not None:
                    data_list.append(tweet)

            if data_list:
                insert_count += insertmany(conn, city, data_list)
            set_metadata(conn, '%s_fetch_end' % city, fetch_end)
            set_metadata(conn, 'last_update', datetime.now())
            logq.put('%-9s %s: fetched: %d, inserted: %d' % (
                capitalize(city), fetch_start.date(), fetch_count, insert_count))

    except KeyboardInterrupt:
        pass


def run_pipeline():
    logfp = open('log-pipeline.txt', 'a')
    logq = Queue()

    def log(s):
        s = '[%s] %s' % (datetime.now().replace(microsecond=0), s)
        print(s)
        sys.stdout.flush()
        logfp.write(s + '\n')
        logfp.flush()

    log('-' * 40)
    log('Starting worker processes...')

    processes = [Process(target=pipeline_worker, args=(city, logq)) for city in CITIES]
    for p in processes:
        p.start()

    try:
        # Main thread: do logging
        while True:
            log(logq.get())

    except KeyboardInterrupt:
        log('Keyboard interrupted! Exiting program...')

    for p in processes:
        p.terminate()
        p.join()

    log('Program exited!')
    logfp.close()


@click.command()
@click.option('--init-database', '-i',
              is_flag=True, default=False,
              help='Initialize and clear the database')
def main(init_database):
    if init_database:
        if click.confirm('Are you sure to initialize the database? This will clear the database if exists!'):
            warnings.filterwarnings('ignore', category=Warning)
            initialize()
        else:
            print('Aborted.')
    else:
        warnings.filterwarnings('ignore', category=Warning)
        run_pipeline()


if __name__ == '__main__':
    freeze_support()
    main()
