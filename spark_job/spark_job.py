from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, sqrt, sin, avg
import time

spark = SparkSession.builder \
    .appName("SparkJob") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Stage 1: generating and caching large dataset...")
print("curl http://localhost:4040/metrics/prometheus NOW")
print("=" * 60)

# generate real data and cache it — memory pressure
df = spark.range(0, 10_000_000, numPartitions=20) \
    .selectExpr(
        "id",
        "id % 1000 AS bucket",
        "cast(rand(23) as double) AS val1",
        "cast(rand(42 * 7) as double) AS val2"
    )

df.cache()
count1 = df.count()
print(f"Cached {count1} rows. Sleeping 30 seconds — curl now!")
time.sleep(30)

print("=" * 60)
print("Stage 2: heavy shuffle — GROUP BY with 1000 buckets...")
print("Watch shuffle metrics appear")
print("=" * 60)

shuffled = df.groupBy("bucket").agg(
    count("*").alias("cnt"),
    avg("val1").alias("avg_val1"),
    _sum("val2").alias("sum_val2")
).orderBy("bucket")

result = shuffled.collect()
print(f"Shuffle complete — {len(result)} buckets. Sleeping 30 seconds...")
time.sleep(30)

print("=" * 60)
print("Stage 3: second shuffle pass — join with itself (more shuffle)...")
print("=" * 60)

df2 = spark.range(0, 5_000_000, numPartitions=20) \
    .selectExpr("id", "id % 1000 AS bucket", "cast(rand(42*3) as double) AS val3")

joined = df.join(df2, on="bucket") \
    .groupBy("bucket") \
    .agg(count("*").alias("total"))

result2 = joined.collect()
print(f"Join complete — {len(result2)} rows. Sleeping 30 seconds...")
time.sleep(30)

print("=" * 60)
print("Stage 4: memory pressure — cache a second large DataFrame")
print("=" * 60)

df3 = spark.range(0, 20_000_000, numPartitions=20) \
    .selectExpr("id", "cast(sqrt(cast(id as double)) as double) AS root")

df3.cache()
count3 = df3.count()
print(f"Second cache: {count3:,} rows. Sleeping 30 seconds...")
time.sleep(30)

print("All stages complete.")
spark.stop()