import warnings
from datetime import datetime

import pymysql

from consts import CITIES
from consts import DATABASE_HOST
from consts import DATABASE_PASSWORD
from consts import DATABASE_PORT
from consts import DATABASE_SCHEMA
from consts import DATABASE_USER


def connect(schema=DATABASE_SCHEMA):
    return pymysql.connect(
        host=DATABASE_HOST,
        user=DATABASE_USER,
        passwd=DATABASE_PASSWORD,
        db=schema,
        port=DATABASE_PORT,
        charset='utf8mb4')


def initialize():
    warnings.filterwarnings('ignore', category=Warning)
    conn = connect(schema=None)
    c = conn.cursor()

    c.execute("""
    DROP DATABASE IF EXISTS `$SCHEMA`;
    """.replace('$SCHEMA', DATABASE_SCHEMA))
    c.execute("""
    CREATE SCHEMA IF NOT EXISTS `$SCHEMA` DEFAULT CHARACTER SET utf8mb4;
    """.replace('$SCHEMA', DATABASE_SCHEMA))

    c.close()
    conn.commit()
    conn.close()

    conn = connect()
    c = conn.cursor()

    # Create table for cities
    for city in CITIES:
        c.execute("""
        CREATE TABLE IF NOT EXISTS $TABLE_NAME (
            `tid` BIGINT UNSIGNED,
            `user_id` BIGINT UNSIGNED,
            `text` VARCHAR(255),
            `create_at` DATETIME,
            `year` SMALLINT UNSIGNED,
            `lang` CHAR(2),
            `lng` FLOAT,
            `lat` FLOAT,
            `sa1_code` BIGINT UNSIGNED,
            `sa2_code` INT UNSIGNED,
            `pos` FLOAT,
            `neu` FLOAT,
            `neg` FLOAT,
            `compound` FLOAT,
            PRIMARY KEY (`tid`),      
            INDEX (`create_at` ASC),
            INDEX (`year` ASC),
            INDEX (`lang` ASC),
            INDEX (`sa1_code` ASC),
            INDEX (`sa2_code` ASC));
        """.replace('$TABLE_NAME', '%s_data' % city))

    # Create table for store other info
    c.execute("""
    CREATE TABLE IF NOT EXISTS `metadata` (
        `key` VARCHAR(255),
        `type` VARCHAR(255),
        `value` VARCHAR(255),
        PRIMARY KEY (`key`));
    """)

    metadata = [('created_at', 'datetime', datetime.now().replace(microsecond=0))]

    for city in ['total'] + CITIES:
        metadata.append(('%s_fetch_end' % city, 'datetime', datetime(2014, 7, 15)))

    c.executemany('INSERT INTO `metadata` VALUES (%s, %s, %s);', metadata)
    c.close()
    conn.commit()
    conn.close()


def get_metadata(conn, key):
    c = conn.cursor()
    c.execute('SELECT `type`, `value` FROM `metadata` WHERE `key` = %s LIMIT 1;', (key,))
    row = c.fetchone()
    c.close()
    if row is None:
        raise KeyError('Unknown metadata key: %s' % key)
    datatype, value = row
    if value is None:
        return None
    elif datatype == 'str':
        return str(value)
    elif datatype == 'int':
        return int(value)
    elif datatype == 'datetime':
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    else:
        raise ValueError('Unknown datatype of metadata: %s: %s' % (key, datatype))


def set_metadata(conn, key, value):
    c = conn.cursor()
    c.execute('SELECT `type` FROM `metadata` WHERE `key` = %s LIMIT 1;', (key,))
    datatype = c.fetchone()[0]
    if datatype is None:
        raise KeyError('Unknown metadata key: %s' % key)
    if datatype == 'datetime':
        value = value.replace(microsecond=0)
    c.execute('UPDATE `metadata` SET `value` = %s WHERE `key` = %s;', (value, key))
    c.close()
    conn.commit()


def insert(conn, city, data):
    return insertmany(conn, city, [data])


def insertmany(conn, city, data_list):
    c = conn.cursor()
    if city not in CITIES:
        raise ValueError('Unknown city: %s' % city)
    rowcount = c.executemany("""
    INSERT IGNORE INTO `$TABLE_NAME` (
    `tid`, `user_id`, `text`, 
    `create_at`, `year`, `lang`,
    `lng`, `lat`, `sa1_code`, `sa2_code`,
    `pos`, `neu`, `neg`, `compound`)
    VALUES (
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s);
    """.replace('$TABLE_NAME', '%s_data' % city), data_list)
    c.close()
    conn.commit()
    return rowcount


if __name__ == '__main__':
    initialize()
