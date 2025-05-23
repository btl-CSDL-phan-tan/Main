# !/usr/bin/python3

import psycopg2
import os

DATABASE_NAME = 'test'

def getopenconnection(user='postgres', password='postgres', dbname='test'):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        host='localhost',
        password=password
    )

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    con = openconnection
    cur = con.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS {ratingstablename};")
        cur.execute(f"CREATE TABLE {ratingstablename} (userid INTEGER, movieid INTEGER, rating FLOAT);")
        with open(ratingsfilepath, 'r', encoding='utf-8') as infile, open('temp_ratings.dat', 'w', encoding='utf-8') as outfile:
            for line in infile:
                parts = line.strip().split('::')
                if len(parts) >= 3:
                    outfile.write('|'.join(parts[:3]) + '\n')
        with open('temp_ratings.dat', 'r', encoding='utf-8') as f:
            cur.copy_from(f, ratingstablename, sep='|', columns=('userid', 'movieid', 'rating'))
        con.commit()
    except Exception as e:
        con.rollback()
        raise Exception(f"Error in loadratings: {str(e)}")
    finally:
        cur.close()
        if os.path.exists('temp_ratings.dat'):
            os.remove('temp_ratings.dat')

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0:
        return
    con = openconnection
    cur = con.cursor()
    try:
        RANGE_TABLE_PREFIX = 'range_part'
        interval = 5.0 / numberofpartitions
        for i in range(numberofpartitions):
            min_range = i * interval
            max_range = (i + 1) * interval if i < numberofpartitions - 1 else 5.0
            table_name = f"{RANGE_TABLE_PREFIX}{i}"
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            cur.execute(f"CREATE TABLE {table_name} (LIKE {ratingstablename} INCLUDING ALL);")
            if i == 0:
                cur.execute(f"INSERT INTO {table_name} SELECT * FROM {ratingstablename} WHERE rating >= {min_range} AND rating <= {max_range};")
            else:
                cur.execute(f"INSERT INTO {table_name} SELECT * FROM {ratingstablename} WHERE rating > {min_range} AND rating <= {max_range};")
        con.commit()
    except Exception as e:
        con.rollback()
        raise Exception(f"Error in rangepartition: {str(e)}")
    finally:
        cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0:
        return
    con = openconnection
    cur = con.cursor()
    try:
        RROBIN_TABLE_PREFIX = 'rrobin_part'
        cur.execute(f"SELECT * FROM {ratingstablename} ORDER BY userid, movieid;")
        rows = cur.fetchall()
        for i in range(numberofpartitions):
            table_name = f"{RROBIN_TABLE_PREFIX}{i}"
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            cur.execute(f"CREATE TABLE {table_name} (LIKE {ratingstablename} INCLUDING ALL);")
        for idx, row in enumerate(rows):
            table_name = f"{RROBIN_TABLE_PREFIX}{idx % numberofpartitions}"
            cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s);", row)
        con.commit()
    except Exception as e:
        con.rollback()
        raise Exception(f"Error in roundrobinpartition: {str(e)}")
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    try:
        RROBIN_TABLE_PREFIX = 'rrobin_part'
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
        total_rows = cur.fetchone()[0]
        n_partitions = count_partitions(RROBIN_TABLE_PREFIX, con)
        if n_partitions == 0:
            return
        index = (total_rows - 1) % n_partitions
        table_name = f"{RROBIN_TABLE_PREFIX}{index}"
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
        con.commit()
    except Exception as e:
        con.rollback()
        raise Exception(f"Error in roundrobininsert: {str(e)}")
    finally:
        cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    try:
        RANGE_TABLE_PREFIX = 'range_part'
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
        n_partitions = count_partitions(RANGE_TABLE_PREFIX, con)
        if n_partitions == 0:
            return
        interval = 5.0 / n_partitions
        index = 0
        for i in range(n_partitions):
            min_range = i * interval
            max_range = (i + 1) * interval if i < n_partitions - 1 else 5.0
            if i == 0:
                if rating >= min_range and rating <= max_range:
                    index = i
                    break
            else:
                if rating > min_range and rating <= max_range:
                    index = i
                    break
        table_name = f"{RANGE_TABLE_PREFIX}{index}"
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
        con.commit()
    except Exception as e:
        con.rollback()
        raise Exception(f"Error in rangeinsert: {str(e)}")
    finally:
        cur.close()

def create_db(dbname):
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='{dbname}'")
        count = cur.fetchone()[0]
        if count == 0:
            cur.execute(f"CREATE DATABASE {dbname}")
        con.commit()
    except Exception as e:
        con.rollback()
        raise Exception(f"Error in create_db: {str(e)}")
    finally:
        cur.close()
        con.close()

def count_partitions(prefix, openconnection):
    con = openconnection
    cur = con.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE '{prefix}%';")
        count = cur.fetchone()[0]
        return count
    except Exception as e:
        raise Exception(f"Error in count_partitions: {str(e)}")
    finally:
        cur.close()