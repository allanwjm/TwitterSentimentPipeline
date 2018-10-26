import sys
import warnings
from datetime import datetime
from datetime import timedelta
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


def run_pipeline():
    warnings.filterwarnings('ignore', category=Warning)
    logfp = open('log-pipeline.txt', 'a')

    def log(s):
        s = '[%s] %s' % (datetime.now().replace(microsecond=0), s)
        print(s)
        sys.stdout.flush()
        logfp.write(s + '\n')
        logfp.flush()

    conn = connect()
    fetch_start = min([get_metadata(conn, '%s_fetch_end' % city) for city in CITIES]) - timedelta(days=1)

    log('-' * 40)
    log('Sentiment analysis pipeline started!')

    try:
        # Main loop
        log('Start fetching from date: %s' % fetch_start)
        while True:
            fetch_start = fetch_start + timedelta(days=1)
            fetch_end = fetch_start + timedelta(days=1)

            # Wait for new data
            while fetch_start > datetime.now() - timedelta(days=7):
                sleep(60 * 60)

            log('Fetching %s...' % fetch_start.date())
            for city in CITIES:
                fetch_count = 0
                insert_count = 0
                fetch_data = fetch(city, fetch_start, fetch_end)
                if fetch_end < get_metadata(conn, '%s_fetch_end' % city):
                    log('%s: skip' % capitalize(city))

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
                log('%s: fetched: %d, inserted: %d' % (capitalize(city), fetch_count, insert_count))

    except KeyboardInterrupt:
        log('Keyboard interrupted! Exiting program...')

    log('Program exited!')
    logfp.close()
    conn.close()


@click.command()
@click.option('--init-database', '-i',
              is_flag=True, default=False,
              help='Initialize and clear the database')
def main(init_database):
    if init_database:
        if click.confirm('Are you sure to initialize the database? This will clear the database if exists!'):
            initialize()
        else:
            print('Aborted.')
    else:
        run_pipeline()


if __name__ == '__main__':
    main()
