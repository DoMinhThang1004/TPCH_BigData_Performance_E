from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# Khởi tạo Spark
spark = SparkSession.builder.appName("TPCH_Tuning").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("1. DỌN DỮ LIỆU ĐỂ TIẾT KIỆM RAM...")
customer = spark.read.csv('/home/uyenn/kl_bigdata/tpch-dbgen/customer.tbl', sep='|')
orders = spark.read.csv('/home/uyenn/kl_bigdata/tpch-dbgen/orders.tbl', sep='|')
lineitem = spark.read.csv('/home/uyenn/kl_bigdata/tpch-dbgen/lineitem.tbl', sep='|')

customer = customer.withColumnRenamed("_c0", "c_custkey").withColumnRenamed("_c6", "c_mktsegment")
orders = orders.withColumnRenamed("_c0", "o_orderkey").withColumnRenamed("_c1", "o_custkey").withColumnRenamed("_c4", "o_orderdate").withColumnRenamed("_c7", "o_shippriority")

# ÉP KIỂU SỐ THỰC (DOUBLE) CHO CỘT TIỀN ĐỂ KHÔNG BỊ LỖI
lineitem = lineitem.withColumnRenamed("_c0", "l_orderkey") \
    .withColumn("l_extendedprice", col("_c5").cast("double")) \
    .withColumn("l_discount", col("_c6").cast("double")) \
    .withColumnRenamed("_c10", "l_shipdate")

print("2. CHUYỂN ĐỔI SANG ĐỊNH DẠNG ORC (YÊU CẦU CỦA ĐỀ BÀI)...")
customer.write.mode("overwrite").orc("/tmp/orc/customer")
orders.write.mode("overwrite").orc("/tmp/orc/orders")
lineitem.write.mode("overwrite").orc("/tmp/orc/lineitem")

spark.read.orc("/tmp/orc/customer").createOrReplaceTempView("customer")
spark.read.orc("/tmp/orc/orders").createOrReplaceTempView("orders")
spark.read.orc("/tmp/orc/lineitem").createOrReplaceTempView("lineitem")

print("3. CHẠY TRUY VẤN (CHƯA TỐI ƯU)...")
query = """
SELECT
    l.l_orderkey,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    o.o_orderdate,
    o.o_shippriority
FROM
    customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON l.l_orderkey = o.o_orderkey
WHERE
    c.c_mktsegment = 'BUILDING'
    AND o.o_orderdate < '1995-03-15'
    AND l.l_shipdate > '1995-03-15'
GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority
ORDER BY revenue DESC, o.o_orderdate
LIMIT 10
"""
start_time = time.time()
result = spark.sql(query)
result.show()
print(f"-> THỜI GIAN CHẠY (CHƯA TỐI ƯU): {time.time() - start_time} giây\n")

print("4. IN EXECUTION PLAN (Để chụp ảnh cho vào báo cáo)...")
result.explain()

print("\n5. ÁP DỤNG TUNING BẰNG KỸ THUẬT BROADCAST JOIN...")
query_tuned = """
SELECT /*+ BROADCAST(c) */
    l.l_orderkey,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    o.o_orderdate,
    o.o_shippriority
FROM
    customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON l.l_orderkey = o.o_orderkey
WHERE
    c.c_mktsegment = 'BUILDING'
    AND o.o_orderdate < '1995-03-15'
    AND l.l_shipdate > '1995-03-15'
GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority
ORDER BY revenue DESC, o.o_orderdate
LIMIT 10
"""
start_time_tuned = time.time()
result_tuned = spark.sql(query_tuned)
result_tuned.show()
print(f"-> THỜI GIAN CHẠY (ĐÃ TUNING BROADCAST JOIN): {time.time() - start_time_tuned} giây\n")

spark.stop()
