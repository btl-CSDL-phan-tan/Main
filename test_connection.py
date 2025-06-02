import psycopg2
from psycopg2 import Error # Hiển thị lỗi khi kết nối nếu có.

def get_open_connection():
    try:
        # Thiết lập thông tin kết nối
        connection = psycopg2.connect(
            dbname="dds_assgn1",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"  # Cổng mặc định của PostgreSQL
        )
        print("Kết nối đến PostgreSQL thành công!")
        return connection
    except Error as e:
        print(f"Lỗi khi kết nối đến PostgreSQL: {e}")
        return None

if __name__ == "__main__":
    # Tạo kết nối
    connection = get_open_connection()

    # if connection:
    #     # Đóng kết nối
    #     connection.close()
    #     print("Đã đóng kết nối!")