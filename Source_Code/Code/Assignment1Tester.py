#
# Tester for the assignment1
#
DATABASE_NAME = 'test'

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'Source_Code/Data/test_data.dat'  # Cập nhật đường dẫn của bạn
ACTUAL_ROWS_IN_INPUT_FILE = 20  # Số dòng trong test_data.dat

import psycopg2
import traceback
import testHelper
import Interface as MyAssignment

if __name__ == '__main__':
    try:
        testHelper.createdb(DATABASE_NAME)

        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            # Test loadratings
            testHelper.deleteAllPublicTables(conn)
            [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            if result:
                print("loadratings function pass!")
            else:
                print("loadratings function fail!", e)

            # psql -U postgres -d test

            # SELECT COUNT(*) FROM ratings;
            # Kết quả trả về phải là 20

            # SELECT * FROM ratings;
            # Trả về các cột: userid, movieid, rating (cột timestamp bị loại bỏ)

            # Test rangepartition
            [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            if result:
                print("rangepartition function pass!")
            else:
                print("rangepartition function fail!", e)

            # Xem các bảng phân vùng:
            # SELECT COUNT(*) FROM range_part0;
            # SELECT COUNT(*) FROM range_part1;
            # SELECT COUNT(*) FROM range_part2;
            # SELECT COUNT(*) FROM range_part3;
            # SELECT COUNT(*) FROM range_part4;
            # Tổng số bản ghi trong các bảng phân vùng phải bằng 20

            # Kiểm tra các phân vùng:
            # SELECT * FROM range_part0 WHERE rating < 0 OR rating > 1;
            # SELECT * FROM range_part1 WHERE rating <= 1 OR rating > 2;
            # SELECT * FROM range_part2 WHERE rating <= 2 OR rating > 3;
            # SELECT * FROM range_part3 WHERE rating <= 3 OR rating > 4;
            # SELECT * FROM range_part4 WHERE rating <= 4 OR rating > 5;
            # Nếu không có dòng nào trả về, phân vùng đúng.

            # Test rangeinsert (đảm bảo các bảng phân vùng đã được tạo)
            [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')
            if result:
                print("rangeinsert function pass!")
            else:
                print("rangeinsert function fail!", e)

            # Tester chèn bản ghi (userid=100, movieid=2, rating=3) vào range_part2.
            # SELECT * FROM range_part2 WHERE userid=100 AND movieid=2 AND rating=3;
            # Phải có 1 bản ghi.

            # Kiểm tra bảng ratings
            # SELECT * FROM ratings WHERE userid=100 AND movieid=2 AND rating=3;
            # Phải có 1 bản ghi.

            # Test roundrobinpartition
            testHelper.deleteAllPublicTables(conn)
            MyAssignment.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)
            [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            if result:
                print("roundrobinpartition function pass!")
            else:
                print("roundrobinpartition function fail!", e)

            # SELECT COUNT(*) FROM rrobin_part0;
            # SELECT COUNT(*) FROM rrobin_part1;
            # SELECT COUNT(*) FROM rrobin_part2;
            # SELECT COUNT(*) FROM rrobin_part3;
            # SELECT COUNT(*) FROM rrobin_part4;
            # Mỗi bảng có 4 - 5 bản ghi. Tổng 21

            # Test roundrobininsert
            [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '0')
            if result:
                print("roundrobininsert function pass!")
            else:
                print("roundrobininsert function fail!", e)

            # Tester chèn bản ghi (userid=100, movieid=1, rating=3) vào rrobin_part0.
            # SELECT * FROM rrobin_part0 WHERE userid=100 AND movieid=1 AND rating=3;
            # Phải có 1 bản ghi.

            # Kiểm tra bảng ratings
            # SELECT * FROM ratings WHERE userid=100 AND movieid=1 AND rating=3;
            # Phải có 1 bản ghi.



            # Kiểm tra tính chất phân vùng:
            # SELECT COUNT(*) FROM (SELECT * FROM range_part0 UNION ALL SELECT * FROM range_part1 UNION ALL SELECT * FROM range_part2 UNION ALL SELECT * FROM range_part3 UNION ALL SELECT * FROM range_part4) AS T;
            # Kết quả phải bằng số bản ghi trong ratings trước khi chạy roundrobinpartition.

            # Kiểm tra tính không giao thoa:
            # SELECT COUNT(*) FROM range_part0 a JOIN range_part1 b ON a.userid = b.userid AND a.movieid = b.movieid AND a.rating = b.rating;
            # Lặp lại cho các cặp bảng khác. Kết quả phải là 0.

            # Kiểm tra tính tái cấu trúc:
            # Đã được kiểm tra trong các bước.

            choice = input('Press enter to Delete all tables? ')
            if choice == '':
                testHelper.deleteAllPublicTables(conn)
            if not conn.close:
                conn.close()

    except Exception as detail:
        traceback.print_exc()