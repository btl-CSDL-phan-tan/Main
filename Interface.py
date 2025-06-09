# !/usr/bin/env python3
#
# Interface for the assignment
#

import psycopg2
# import time
# import logging
import os

DATABASE_NAME = 'dds_assgn1'

# # Thiết lập logging
# logging.basicConfig(
#     filename='execution_times.log',
#     level=logging.INFO,
#     format='%(asctime)s - %(message)s',
#     datefmt='%Y-%m-%d %H:%M:%S'
# )

def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    """
    Establish a connection to the PostgreSQL database.
    
    :param user: Database user (default: 'postgres')
    :param password: Database password (default: 'postgres')
    :param dbname: Database name (default: 'postgres')
    :return: psycopg2 connection object
    """
    return psycopg2.connect(f"dbname='{dbname}' user='{user}' host='localhost' password='{password}'")

def create_db(dbname):
    """
    Create a database if it does not already exist.
    
    :param dbname: Name of the database to create
    :return: None
    """
    # start_time = time.perf_counter()
    try:
        con = getopenconnection(dbname='postgres')
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute(f"SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='{dbname}'")
        count = cur.fetchone()[0]
        if count == 0:
            cur.execute(f"CREATE DATABASE {dbname}")
        cur.close()
        con.close()
    except psycopg2.Error as e:
        # logging.error(f"Database error in create_db: {e}")
        raise
    # end_time = time.perf_counter()
    # logging.info(f"create_db executed in {end_time - start_time:.6f} seconds")

def setup_metadata(openconnection):
    """
    Create the metadata table to store partitioning information.
    
    :param openconnection: Existing database connection
    :return: None
    """
    # start_time = time.perf_counter()
    try:
        cur = openconnection.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                partition_type VARCHAR(20) PRIMARY KEY,
                numberofpartitions INTEGER,
                delta FLOAT DEFAULT 0,
                row_count BIGINT DEFAULT 0
            );
        """)
        cur.execute("DELETE FROM metadata;")
        openconnection.commit()
        cur.close()
    except psycopg2.Error as e:
        # logging.error(f"Database error in setup_metadata: {e}")
        raise
    # end_time = time.perf_counter()
    # logging.info(f"setup_metadata executed in {end_time - start_time:.6f} seconds")

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    
    :param ratingstablename: Name of the table to load data into
    :param ratingsfilepath: Path to the ratings data file
    :param openconnection: Existing database connection
    :return: None
    """
    # start_time = time.perf_counter()
    try:
        if not os.path.exists(ratingsfilepath):
            # logging.error(f"The file at {ratingsfilepath} does not exist.")
            raise FileNotFoundError(f"The file at {ratingsfilepath} does not exist.")
        
        con = openconnection
        cur = con.cursor()
        
        setup_metadata(openconnection)
        
        cur.execute(f"""
            DROP TABLE IF EXISTS {ratingstablename};
            CREATE TABLE {ratingstablename} (
                userid INTEGER,
                extra1 CHAR(1),
                movieid INTEGER,
                extra2 CHAR(1),
                rating FLOAT CHECK (rating >= 0 AND rating <= 5),
                extra3 CHAR(1),
                timestamp BIGINT
            );
        """)
        
        cur.copy_from(open(ratingsfilepath, 'r', encoding='utf-8'), ratingstablename, sep=':', columns=(
            'userid', 'extra1', 'movieid', 'extra2', 'rating', 'extra3', 'timestamp'
        ))
        
        cur.execute(f"""
            ALTER TABLE {ratingstablename}
            DROP COLUMN extra1,
            DROP COLUMN extra2,
            DROP COLUMN extra3,
            DROP COLUMN timestamp;
        """)
        
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
        row_count = cur.fetchone()[0]
        cur.execute("""
            INSERT INTO metadata (partition_type, row_count)
            VALUES ('roundrobin', %s)
            ON CONFLICT (partition_type) DO UPDATE
            SET row_count = EXCLUDED.row_count;
        """, (row_count,))
        
        con.commit()
        cur.close()
    except (psycopg2.Error, FileNotFoundError) as e:
        # logging.error(f"Error in loadratings: {e}")
        raise
    # end_time = time.perf_counter()
    # logging.info(f"loadratings executed in {end_time - start_time:.6f} seconds, loaded {row_count} rows")

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    
    :param ratingstablename: Name of the main table
    :param numberofpartitions: Number of partitions to create
    :param openconnection: Existing database connection
    :return: None
    """
    # start_time = time.perf_counter()
    try:
        if numberofpartitions <= 0:
            # logging.warning("numberofpartitions <= 0, skipping rangepartition.")
            return
        
        con = openconnection
        cur = con.cursor()
        
        setup_metadata(openconnection)
        
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_rating ON {ratingstablename}(rating);")
        
        delta = 5.0 / numberofpartitions
        for i in range(numberofpartitions):
            table_name = f"range_part{i}"
            min_range = i * delta
            max_range = min_range + delta if i < numberofpartitions - 1 else 5.0
            if i == 0:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} AS
                    SELECT userid, movieid, rating
                    FROM {ratingstablename}
                    WHERE rating >= {min_range} AND rating <= {max_range};
                """)
            else:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} AS
                    SELECT userid, movieid, rating
                    FROM {ratingstablename}
                    WHERE rating > {min_range} AND rating <= {max_range};
                """)
            cur.execute(f"""
                ALTER TABLE {table_name}
                ADD CONSTRAINT check_rating_{i} CHECK (rating >= 0 AND rating <= 5);
            """)
        
        cur.execute("""
            INSERT INTO metadata (partition_type, numberofpartitions, delta)
            VALUES ('range', %s, %s)
            ON CONFLICT (partition_type) DO UPDATE
            SET numberofpartitions = EXCLUDED.numberofpartitions, delta = EXCLUDED.delta;
        """, (numberofpartitions, delta))
        
        con.commit()
        cur.close()
    except psycopg2.Error as e:
        # logging.error(f"Database error in rangepartition: {e}")
        raise
    # end_time = time.perf_counter()
    # logging.info(f"rangepartition executed in {end_time - start_time:.6f} seconds with {numberofpartitions} partitions")

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    
    :param ratingstablename: Name of the main table
    :param numberofpartitions: Number of partitions to create
    :param openconnection: Existing database connection
    :return: None
    """
    # start_time = time.perf_counter()
    try:
        if numberofpartitions <= 0:
            # logging.warning("numberofpartitions <= 0, skipping roundrobinpartition.")
            return
        
        con = openconnection
        cur = con.cursor()
        
        setup_metadata(openconnection)
        
        for i in range(numberofpartitions):
            table_name = f"rrobin_part{i}"
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS
                SELECT userid, movieid, rating
                FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER () - 1 AS rnum
                    FROM {ratingstablename}) AS temp
                WHERE rnum % {numberofpartitions} = {i};
            """)
            cur.execute(f"""
                ALTER TABLE {table_name}
                ADD CONSTRAINT check_rating_{i} CHECK (rating >= 0 AND rating <= 5);
            """)
        
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
        row_count = cur.fetchone()[0]
        cur.execute("""
            INSERT INTO metadata (partition_type, numberofpartitions, row_count)
            VALUES ('roundrobin', %s, %s)
            ON CONFLICT (partition_type) DO UPDATE
            SET numberofpartitions = EXCLUDED.numberofpartitions, row_count = EXCLUDED.row_count;
        """, (numberofpartitions, row_count))
        
        con.commit()
        cur.close()
    except psycopg2.Error as e:
        # logging.error(f"Database error in roundrobinpartition: {e}")
        raise
    # end_time = time.perf_counter()
    # logging.info(f"roundrobinpartition executed in {end_time - start_time:.6f} seconds with {numberofpartitions} partitions")

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin approach.
    
    :param ratingstablename: Name of the main table
    :param userid: User ID of the new rating
    :param itemid: Movie ID of the new rating
    :param rating: Rating value
    :param openconnection: Existing database connection
    :return: None
    """
    # start_time = time.perf_counter()
    try:
        con = openconnection
        cur = con.cursor()
        
        cur.execute(f"""
            INSERT INTO {ratingstablename} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))
        
        cur.execute("SELECT row_count, numberofpartitions FROM metadata WHERE partition_type = 'roundrobin';")
        result = cur.fetchone()
        if not result:
            # logging.error("Metadata for round-robin not found.")
            raise Exception("Metadata for round-robin not found.")
        row_count, numberofpartitions = result
        
        index = row_count % numberofpartitions
        table_name = f"rrobin_part{index}"
        
        cur.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))
        
        cur.execute("""
            UPDATE metadata
            SET row_count = row_count + 1
            WHERE partition_type = 'roundrobin';
        """)
        
        con.commit()
        cur.close()
    except psycopg2.Error as e:
        # logging.error(f"Database error in roundrobininsert: {e}")
        raise
    # end_time = time.perf_counter()
    # logging.info(f"roundrobininsert executed in {end_time - start_time:.6f} seconds")

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    
    :param ratingstablename: Name of the main table
    :param userid: User ID of the new rating
    :param itemid: Movie ID of the new rating
    :param rating: Rating value
    :param openconnection: Existing database connection
    :return: None
    """
    # start_time = time.perf_counter()
    try:
        con = openconnection
        cur = con.cursor()
        
        cur.execute(f"""
            INSERT INTO {ratingstablename} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))
        
        cur.execute("SELECT numberofpartitions, delta FROM metadata WHERE partition_type = 'range';")
        result = cur.fetchone()
        if not result:
            # logging.error("Metadata for range partitioning not found.")
            raise Exception("Metadata for range partitioning not found.")
        numberofpartitions, delta = result
        
        index = int(rating / delta)
        if rating % delta == 0 and index > 0:
            index -= 1
        index = min(index, numberofpartitions - 1)
        table_name = f"range_part{index}"
        
        cur.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))
        
        con.commit()
        cur.close()
    except psycopg2.Error as e:
        # logging.error(f"Database error in rangeinsert: {e}")
        raise
    # end_time = time.perf_counter()
    # logging.info(f"rangeinsert executed in {end_time - start_time:.6f} seconds")

def count_partitions(prefix, openconnection):
    """
    Count the number of user tables whose names start with the given prefix.

    :param prefix: Prefix of the table names to count
    :param openconnection: Existing database connection
    :return: Number of tables with the given prefix
    """
    # start_time = time.perf_counter()
    try:
        cur = openconnection.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM pg_catalog.pg_tables
            WHERE schemaname = 'public' AND tablename LIKE %s;
        """, (prefix + '%',))
        count = cur.fetchone()[0]
        cur.close()
    except psycopg2.Error as e:
        # logging.error(f"Database error in count_partitions: {e}")
        raise
    # end_time = time.perf_counter()
    # logging.info(f"count_partitions executed in {end_time - start_time:.6f} seconds")
    return count