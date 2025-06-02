# !/usr/bin/env python3
#
# Interface for the assignment
#

import psycopg2
import os

DATABASE_NAME = 'dds_assgn1'

# Thay đổi lại mật khẩu để khớp với cấu hình của thầy. Password của nhóm là: 'postgres'
def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect(f"dbname='{dbname}' user='{user}' host='localhost' password='{password}'")

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """
    con = openconnection
    cur = con.cursor()
    
    # Kiểm tra tệp đầu vào tồn tại
    if not os.path.exists(ratingsfilepath):
        return
    
    # Tạo CSDL nếu chưa tồn tại
    create_db(DATABASE_NAME)
    
    # Xóa bảng cũ nếu tồn tại
    cur.execute(f"DROP TABLE IF EXISTS {ratingstablename};")
    
    # Tạo bảng với cột tạm và ràng buộc rating từ 0 đến 5
    cur.execute(f"""
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
    
    # Tải dữ liệu trực tiếp từ tệp
    cur.copy_from(open(ratingsfilepath, 'r', encoding='utf-8'), ratingstablename, sep=':', columns=(
        'userid', 'extra1', 'movieid', 'extra2', 'rating', 'extra3', 'timestamp'
    ))
    
    # Xóa các cột không cần thiết
    cur.execute(f"""
        ALTER TABLE {ratingstablename}
        DROP COLUMN extra1,
        DROP COLUMN extra2,
        DROP COLUMN extra3,
        DROP COLUMN timestamp;
    """)
    
    con.commit()
    cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    con = openconnection
    cur = con.cursor()
    delta = 5.0 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'
    for i in range(numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta if i < numberofpartitions - 1 else 5.0  # Đảm bảo maxRange không vượt 5
        table_name = f"{RANGE_TABLE_PREFIX}{i}"
        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")
        if i == 0:
            cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) SELECT userid, movieid, rating FROM {ratingstablename} WHERE rating >= {minRange} AND rating <= {maxRange};")
        else:
            cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) SELECT userid, movieid, rating FROM {ratingstablename} WHERE rating > {minRange} AND rating <= {maxRange};")
        con.commit()
    cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    for i in range(numberofpartitions):
        table_name = f"{RROBIN_TABLE_PREFIX}{i}"
        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) SELECT userid, movieid, rating FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER () AS rnum FROM {ratingstablename}) AS temp WHERE MOD(temp.rnum-1, {numberofpartitions}) = {i};")
        con.commit()
    cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES ({userid}, {itemid}, {rating});")
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
    total_rows = cur.fetchone()[0]
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    index = (total_rows - 1) % numberofpartitions
    table_name = f"{RROBIN_TABLE_PREFIX}{index}"
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES ({userid}, {itemid}, {rating});")
    con.commit()
    cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5.0 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index > 0:
        index -= 1
    table_name = f"{RANGE_TABLE_PREFIX}{index}"
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES ({userid}, {itemid}, {rating});")
    con.commit()
    cur.close()

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return: None
    """
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute(f"SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='{dbname}'")
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute(f"CREATE DATABASE {dbname}")
    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute(f"SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE '{prefix}%';")
    count = cur.fetchone()[0]
    cur.close()
    return count