import traceback
import psycopg2

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

def createdb(dbname):
    con = getopenconnection()
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='{dbname}'")
        count = cur.fetchone()[0]
        if count == 0:
            cur.execute(f"CREATE DATABASE {dbname}")
        else:
            print(f'A database named "{dbname}" already exists')
        con.commit()
    except Exception as e:
        con.rollback()
        raise Exception(f"Error in createdb: {str(e)}")
    finally:
        cur.close()
        con.close()

def delete_db(dbname):
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    try:
        cur.execute(f"DROP DATABASE IF EXISTS {dbname}")
        con.commit()
    except Exception as e:
        con.rollback()
        raise Exception(f"Error in delete_db: {str(e)}")
    finally:
        cur.close()
        con.close()

def deleteAllPublicTables(openconnection):
    try:
        cur = openconnection.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = [row[0] for row in cur]
        for tablename in tables:
            cur.execute(f"DROP TABLE IF EXISTS {tablename} CASCADE")
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise Exception(f"Error in deleteAllPublicTables: {str(e)}")
    finally:
        cur.close()

def getopenconnection(user='postgres', password='postgres', dbname='postgres'):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        host='localhost',
        password=password
    )

def check_completeness_disjointness(ratingstablename, prefix, num_partitions, openconnection, expected_rows):
    cur = openconnection.cursor()
    try:
        # Kiểm tra tính hoàn chỉnh (completeness) và tái cấu trúc (reconstruction)
        selects = [f'SELECT * FROM {prefix}{i}' for i in range(num_partitions)]
        cur.execute(f"SELECT COUNT(*) FROM ({' UNION ALL '.join(selects)}) AS T")
        total_in_partitions = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
        total_in_main = cur.fetchone()[0]
        if total_in_partitions != expected_rows or total_in_main != expected_rows:
            return False, f"Completeness/Reconstruction failed: Expected {expected_rows} rows, found {total_in_partitions} in partitions, {total_in_main} in main table"

        # Kiểm tra tính không giao thoa (disjointness)
        for i in range(num_partitions):
            for j in range(i + 1, num_partitions):
                cur.execute(f"SELECT COUNT(*) FROM {prefix}{i} a JOIN {prefix}{j} b ON a.userid = b.userid AND a.movieid = b.movieid AND a.rating = b.rating")
                overlap = cur.fetchone()[0]
                if overlap > 0:
                    return False, f"Disjointness failed: Overlap between {prefix}{i} and {prefix}{j}"
        return True, None
    finally:
        cur.close()

def testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE):
    try:
        MyAssignment.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {RATINGS_TABLE}")
        count = cur.fetchone()[0]
        if count == ACTUAL_ROWS_IN_INPUT_FILE:
            return [True, None]
        else:
            return [False, f"Expected {ACTUAL_ROWS_IN_INPUT_FILE} rows but got {count}"]
    except Exception as e:
        return [False, str(e)]

def testrangepartition(MyAssignment, RATINGS_TABLE, num_partitions, conn, min_val, max_val):
    try:
        MyAssignment.rangepartition(RATINGS_TABLE, num_partitions, conn)
        cur = conn.cursor()
        # Kiểm tra số bảng partition
        partition_tables = []
        for i in range(num_partitions):
            table_name = f"{RANGE_TABLE_PREFIX}{i}"
            cur.execute(f"SELECT to_regclass('{table_name}')")
            if cur.fetchone()[0] is not None:
                partition_tables.append(table_name)
        if len(partition_tables) != num_partitions:
            return [False, f"Expected {num_partitions} partitions but found {len(partition_tables)}"]

        # Kiểm tra phân vùng range
        interval = 5.0 / num_partitions
        for i in range(num_partitions):
            min_range = i * interval
            max_range = (i + 1) * interval if i < num_partitions - 1 else 5.0
            cur.execute(f"SELECT * FROM {RANGE_TABLE_PREFIX}{i}")
            rows = cur.fetchall()
            for row in rows:
                rating = row[2]  # Cột rating
                if i == 0:
                    if not (rating >= min_range and rating <= max_range):
                        return [False, f"Row in {RANGE_TABLE_PREFIX}{i} has rating {rating} outside range [{min_range}, {max_range}]"]
                else:
                    if not (rating > min_range and rating <= max_range):
                        return [False, f"Row in {RANGE_TABLE_PREFIX}{i} has rating {rating} outside range ({min_range}, {max_range}]"]

        # Kiểm tra tính hoàn chỉnh, không giao thoa, tái cấu trúc
        result, error = check_completeness_disjointness(RATINGS_TABLE, RANGE_TABLE_PREFIX, num_partitions, conn, max_val)
        if not result:
            return [False, error]

        return [True, None]
    except Exception as e:
        return [False, str(e)]

def testrangeinsert(MyAssignment, RATINGS_TABLE, userid, movieid, rating, conn, expected_partition):
    try:
        MyAssignment.rangeinsert(RATINGS_TABLE, userid, movieid, rating, conn)
        cur = conn.cursor()
        table_name = f"{RANGE_TABLE_PREFIX}{expected_partition}"
        cur.execute(f"SELECT * FROM {table_name} WHERE userid=%s AND movieid=%s AND rating=%s", (userid, movieid, rating))
        row = cur.fetchone()
        if row:
            return [True, None]
        else:
            return [False, f"Row ({userid}, {movieid}, {rating}) not found in {table_name}"]
    except Exception as e:
        return [False, str(e)]

def testroundrobinpartition(MyAssignment, RATINGS_TABLE, num_partitions, conn, min_val, max_val):
    try:
        MyAssignment.roundrobinpartition(RATINGS_TABLE, num_partitions, conn)
        cur = conn.cursor()
        partition_tables = []
        for i in range(num_partitions):
            table_name = f"{RROBIN_TABLE_PREFIX}{i}"
            cur.execute(f"SELECT to_regclass('{table_name}')")
            if cur.fetchone()[0] is not None:
                partition_tables.append(table_name)
        if len(partition_tables) != num_partitions:
            return [False, f"Expected {num_partitions} partitions but found {len(partition_tables)}"]

        # Kiểm tra phân phối đều
        counts = []
        for i in range(num_partitions):
            cur.execute(f"SELECT COUNT(*) FROM {RROBIN_TABLE_PREFIX}{i}")
            counts.append(cur.fetchone()[0])
        avg = max_val / num_partitions
        for i, count in enumerate(counts):
            if not (avg - 1 <= count <= avg + 1):
                return [False, f"Uneven distribution in {RROBIN_TABLE_PREFIX}{i}: {count} rows (expected around {avg})"]

        # Kiểm tra tính hoàn chỉnh, không giao thoa, tái cấu trúc
        result, error = check_completeness_disjointness(RATINGS_TABLE, RROBIN_TABLE_PREFIX, num_partitions, conn, max_val)
        if not result:
            return [False, error]

        return [True, None]
    except Exception as e:
        return [False, str(e)]

def testroundrobininsert(MyAssignment, RATINGS_TABLE, userid, movieid, rating, conn, expected_partition):
    try:
        MyAssignment.roundrobininsert(RATINGS_TABLE, userid, movieid, rating, conn)
        cur = conn.cursor()
        table_name = f"{RROBIN_TABLE_PREFIX}{expected_partition}"
        cur.execute(f"SELECT * FROM {table_name} WHERE userid=%s AND movieid=%s AND rating=%s", (userid, movieid, rating))
        row = cur.fetchone()
        if row:
            return [True, None]
        else:
            return [False, f"Row ({userid}, {movieid}, {rating}) not found in {table_name}"]
    except Exception as e:
        return [False, str(e)]