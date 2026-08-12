from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
import os

# 1. Dọn dẹp dữ liệu cũ và tạo một thư mục Temp riêng trên ổ 1TB để Spark không xả rác vào /tmp của Linux
os.system("rm -rf spark-warehouse /home/uyenn/kl_bigdata/orc")
os.system("mkdir -p /home/uyenn/kl_bigdata/spark-temp") 

# 2. Cấu hình Spark: Bật Bucketing, trỏ bộ đệm temp về ổ 1TB và cấp thêm RAM
spark = SparkSession.builder.appName("TPCH_Spark_Final") \
    .config("spark.sql.sources.bucketing.enabled", "true") \
    .config("spark.local.dir", "/home/uyenn/kl_bigdata/spark-temp") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("1. NẠP DỮ LIỆU & ÉP KIỂU SỐ THỰC...")
customer = spark.read.csv('/home/uyenn/kl_bigdata/tpch-dbgen/customer.tbl', sep='|').withColumnRenamed("_c0", "c_custkey").withColumnRenamed("_c6", "c_mktsegment")
orders = spark.read.csv('/home/uyenn/kl_bigdata/tpch-dbgen/orders.tbl', sep='|').withColumnRenamed("_c0", "o_orderkey").withColumnRenamed("_c1", "o_custkey").withColumnRenamed("_c4", "o_orderdate").withColumnRenamed("_c5", "o_orderpriority").withColumnRenamed("_c7", "o_shippriority")
lineitem = spark.read.csv('/home/uyenn/kl_bigdata/tpch-dbgen/lineitem.tbl', sep='|') \
    .withColumnRenamed("_c0", "l_orderkey") \
    .withColumn("l_quantity", col("_c4").cast("double")) \
    .withColumn("l_extendedprice", col("_c5").cast("double")) \
    .withColumn("l_discount", col("_c6").cast("double")) \
    .withColumnRenamed("_c8", "l_returnflag").withColumnRenamed("_c9", "l_linestatus") \
    .withColumnRenamed("_c10", "l_shipdate").withColumnRenamed("_c11", "l_commitdate") \
    .withColumnRenamed("_c12", "l_receiptdate").withColumnRenamed("_c14", "l_shipmode")

print("2. TỐI ƯU HÓA: LƯU ORC + BUCKETED + PARTITION (Sẽ mất 15-30 phút)...")
customer.write.mode("overwrite").orc("/home/uyenn/kl_bigdata/orc/customer")

# Lưu bảng orders với 10 Bucket
orders.write.mode("overwrite").bucketBy(10, "o_orderkey").sortBy("o_orderkey").saveAsTable("bucketed_orders", format="orc", path="/home/uyenn/kl_bigdata/orc/orders")

# Lưu bảng lineitem kết hợp Partition và 10 Bucket
lineitem.write.mode("overwrite").partitionBy("l_returnflag").bucketBy(10, "l_orderkey").sortBy("l_orderkey").saveAsTable("bucketed_lineitem", format="orc", path="/home/uyenn/kl_bigdata/orc/lineitem")

print("3. NẠP DỮ LIỆU VÀO SPARK SQL VÀ CHẠY 5 CÂU TRUY VẤN...")
spark.sql("REFRESH TABLE bucketed_orders")
spark.sql("REFRESH TABLE bucketed_lineitem")
customer.createOrReplaceTempView("customer")

queries = [
    "SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice) FROM bucketed_lineitem WHERE l_shipdate <= '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus",
    "SELECT SUM(l_extendedprice * l_discount) FROM bucketed_lineitem WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24",
    "SELECT o_orderpriority, COUNT(*) FROM bucketed_orders o JOIN bucketed_lineitem l ON o.o_orderkey = l.l_orderkey WHERE o.o_orderdate >= '1993-07-01' AND o.o_orderdate < '1993-10-01' GROUP BY o_orderpriority ORDER BY o_orderpriority",
    "SELECT /*+ BROADCAST(c) */ l.l_orderkey, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue FROM customer c JOIN bucketed_orders o ON c.c_custkey = o.o_custkey JOIN bucketed_lineitem l ON l.l_orderkey = o.o_orderkey WHERE c.c_mktsegment = 'BUILDING' AND o.o_orderdate < '1995-03-15' AND l.l_shipdate > '1995-03-15' GROUP BY l.l_orderkey ORDER BY revenue DESC LIMIT 10",
    "SELECT l.l_shipmode, SUM(CASE WHEN o.o_orderpriority = '1-URGENT' OR o.o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) FROM bucketed_orders o JOIN bucketed_lineitem l ON o.o_orderkey = l.l_orderkey WHERE l.l_shipmode IN ('MAIL', 'SHIP') AND l.l_commitdate < l.l_receiptdate AND l.l_shipdate < l.l_commitdate GROUP BY l.l_shipmode ORDER BY l.l_shipmode"
]

for i, q in enumerate(queries, 1):
    start = time.time()
    spark.sql(q).collect()
    print(f"-> Thời gian Query {i} (Tuning: Bucketed, Partition, Broadcast): {time.time() - start:.2f} giây")

spark.stop()