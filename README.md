# Đánh giá hiệu năng hệ thống CSDL: Apache Spark vs DuckDB

Đây là kho lưu trữ mã nguồn và kịch bản thực nghiệm phục vụ cho đồ án khóa luận, đánh giá hiệu năng giữa kiến trúc xử lý phân tán (Apache Spark SQL) và kiến trúc xử lý cục bộ (DuckDB) trên bộ tiêu chuẩn TPC-H.

## Công cụ & Công nghệ
- **Môi trường:** Ubuntu (WSL) trên Windows.
- **Ngôn ngữ:** Python (PySpark), SQL.
- **Hệ quản trị CSDL:** Apache Spark 3.x, DuckDB 1.0.0.
- **Dataset:** TPC-H Benchmark (Scale Factor = 10, dung lượng ~10GB).

## Cấu trúc thư mục
- `run_spark_final.py`: Kịch bản thực thi Apache Spark SQL.
- `duckdb_queries.sql`: Kịch bản khởi tạo và truy vấn trên DuckDB.
- `setup_commands.sh`: Kịch bản sinh dữ liệu TPC-H.

## Hướng dẫn thực nghiệm
1. **Sinh dữ liệu:** Chạy `./setup_commands.sh` để sinh bộ dữ liệu 10GB.
2. **Thực thi DuckDB:** Sử dụng lệnh trong `duckdb_queries.sql` để nạp dữ liệu và truy vấn.
3. **Thực thi Spark:** Chạy `python3 run_spark_final.py` để thực hiện benchmark.
