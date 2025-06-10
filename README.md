# Distributed Database Assignment - Table Partitioning

**Bài tập nhóm môn Cơ sở Dữ liệu Phân tán (Distributed Database Systems)**

## 📋 Mô tả dự án

Dự án này triển khai các chiến lược phân vùng bảng (table partitioning) trong PostgreSQL sử dụng Python, bao gồm:

- **Range Partitioning**: Phân vùng dựa trên khoảng giá trị rating (0-5)
- **Round-Robin Partitioning**: Phân vùng theo thuật toán round-robin
- **Insert Operations**: Các thao tác chèn dữ liệu vào đúng phân vùng
- **Testing Framework**: Hệ thống kiểm thử toàn diện

## 🗂️ Cấu trúc dự án

```
Main/
├── Interface.py              # Module chính chứa các hàm partitioning
├── Assignment1Tester.py      # Script test chính
├── testHelper.py            # Các hàm hỗ trợ testing
├── test_connection.py       # Test kết nối database
├── test_data.dat           # Dữ liệu test mẫu
├── test.txt                # File ghi chú
├── Source_Code/            # Thư mục source code
│   ├── Code/              # Code chính
│   └── Data/              # Dữ liệu test
├── Report/                # Báo cáo
│   └── Báo Cáo.pdf       # Báo cáo chi tiết
└── README.md              # File này
```

## 🚀 Cài đặt và Thiết lập

### Yêu cầu hệ thống

- **Python 3.x**
- **PostgreSQL** (phiên bản 10+)
- **psycopg2** library
- **ratings.dat** file (>100MB, không upload được lên git)

### Cài đặt dependencies

```bash
pip install psycopg2-binary
```

### Cấu hình database

1. Tạo PostgreSQL server với thông tin:
   - Host: `localhost`
   - Port: `5432` (default)
   - Username: `postgres`
   - Password: `1234`

2. Đảm bảo PostgreSQL service đang chạy

## 💻 Sử dụng

### 1. Chạy test chính

```bash
python Assignment1Tester.py
```

### 2. Test kết nối database

```bash
python test_connection.py
```

### 3. Sử dụng từng chức năng

```python
import Interface as MyAssignment

# Tạo database
MyAssignment.create_db('dds_assgn1')

# Kết nối database
conn = MyAssignment.getopenconnection(dbname='dds_assgn1')

# Load dữ liệu ratings
MyAssignment.loadratings('ratings', 'ratings.dat', conn)

# Tạo range partitions (5 phân vùng)
MyAssignment.rangepartition('ratings', 5, conn)

# Tạo round-robin partitions (5 phân vùng)
MyAssignment.roundrobinpartition('ratings', 5, conn)

# Insert dữ liệu mới
MyAssignment.rangeinsert('ratings', 100, 200, 4.5, conn)
MyAssignment.roundrobininsert('ratings', 101, 201, 3.5, conn)
```

## 🔧 Các chức năng chính

### 1. **loadratings()**
- Load dữ liệu từ file `ratings.dat` vào bảng chính
- Tạo bảng metadata để lưu thông tin partitioning
- Xử lý format dữ liệu: `userid:extra:movieid:extra:rating:extra:timestamp`

### 2. **rangepartition()**
- Tạo phân vùng dựa trên khoảng giá trị rating
- Chia đều khoảng 0-5 thành N phân vùng
- Tạo các bảng: `range_part0`, `range_part1`, ..., `range_partN-1`

### 3. **roundrobinpartition()**
- Tạo phân vùng theo thuật toán round-robin
- Phân phối đều dữ liệu vào N phân vùng
- Tạo các bảng: `rrobin_part0`, `rrobin_part1`, ..., `rrobin_partN-1`

### 4. **rangeinsert() & roundrobininsert()**
- Chèn dữ liệu mới vào bảng chính và phân vùng phù hợp
- Tự động xác định phân vùng đích dựa trên giá trị rating hoặc round-robin

### 5. **Testing Framework**
- Kiểm thử tự động tất cả các chức năng
- Xác minh tính đúng đắn của partitioning
- Báo cáo kết quả chi tiết

## 📊 Dữ liệu

- **Input file**: `ratings.dat` (10,000,054 dòng dữ liệu)
- **Format**: `userid:extra:movieid:extra:rating:extra:timestamp`
- **Rating range**: 0.0 - 5.0
- **File size**: >100MB (không thể upload lên git)

## 🧪 Test Results

Khi chạy `Assignment1Tester.py`, bạn sẽ thấy output:

```
loadratings function pass!
rangepartition function pass!
rangeinsert function pass!
roundrobinpartition function pass!
roundrobininsert function pass!
```

## 📈 Performance

- **Metadata tracking**: Theo dõi số lượng partitions, delta values, row counts
- **Index optimization**: Tạo index trên cột rating cho range queries
- **Constraint validation**: Kiểm tra tính hợp lệ của rating (0-5)
- **Execution logging**: Có thể bật logging để theo dõi thời gian thực thi

## ⚠️ Lưu ý quan trọng

1. **Database credentials**: Thay đổi username/password trong `getopenconnection()` nếu cần
2. **File path**: Đảm bảo `ratings.dat` nằm trong thư mục gốc
3. **Memory usage**: File ratings.dat lớn, cần đủ RAM để xử lý
4. **PostgreSQL version**: Đảm bảo sử dụng PostgreSQL 10+ để hỗ trợ đầy đủ các tính năng

## 🤝 Thành viên nhóm

- Thông tin về thành viên nhóm và phân công công việc chi tiết có trong file `Report/Báo Cáo.pdf`

## 📚 Tài liệu tham khảo

- PostgreSQL Documentation: Table Partitioning
- psycopg2 Documentation
- Python Database Programming

## 🐛 Troubleshooting

### Lỗi kết nối database
```python
# Kiểm tra PostgreSQL service
# Xác minh credentials
# Test với test_connection.py
```

### Lỗi file không tồn tại
```bash
# Đảm bảo ratings.dat nằm trong thư mục gốc
# Kiểm tra quyền đọc file
```

### Lỗi memory
```bash
# Tăng shared_buffers trong postgresql.conf
# Sử dụng môi trường có RAM đủ lớn
```

---

**Liên hệ**: Nếu có thắc mắc về code hoặc báo cáo, vui lòng tham khảo file `Report/Báo Cáo.pdf` hoặc liên hệ các thành viên trong nhóm.
