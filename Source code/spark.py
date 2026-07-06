from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, sum as spark_sum, count, when, round as spark_round
import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

args = sys.argv
inp = args[1]
out = args[2]

file_paths = inp.split(",")

data_table = StructType([
    StructField("driverID", StringType(), False),
    StructField("carPlateNumber", StringType(), False),
    StructField("Latitude", DoubleType(), False),
    StructField("Longtitude", DoubleType(), False),
    StructField("Speed", IntegerType(), False),
    StructField("Direction", IntegerType(), False),
    StructField("siteName", StringType(), True),
    StructField("Time", StringType(), False),
    StructField("isRapidlySpeedup", IntegerType(), True),
    StructField("isRapidlySlowdown", IntegerType(), True),
    StructField("isNeutralSlide", IntegerType(), True),
    StructField("isNeutralSlideFinished", IntegerType(), True),
    StructField("neutralSlideTime", DoubleType(), True),
    StructField("isOverspeed", IntegerType(), True),
    StructField("isOverspeedFinished", IntegerType(), True),
    StructField("overspeedTime", DoubleType(), True),
    StructField("isFatigueDriving", IntegerType(), True),
    StructField("isHthrottleStop", IntegerType(), True),
    StructField("isOilLeak", IntegerType(), True)
])

read_Files = spark.read.option("header", "false").schema(data_table).csv(file_paths)

read_Files = read_Files.fillna(0, subset=[
    "isRapidlySpeedup", "isRapidlySlowdown", "isNeutralSlide",
    "isNeutralSlideFinished", "isOverspeed", "isOverspeedFinished",
    "isFatigueDriving", "isHthrottleStop", "isOilLeak",
    "neutralSlideTime", "overspeedTime"
])

data_summary = read_Files.groupBy("driverID", "carPlateNumber").agg(
    count(when(col("isOverspeed") == 1, 1)).alias("overspeed_count"),
    spark_round(spark_sum("overspeedTime") / 60, 2).alias("overspeed_time_min"),
    spark_round(spark_sum("neutralSlideTime") / 60, 2).alias("neutral_slide_time_min"),
    count(when(col("isRapidlySpeedup") == 1, 1)).alias("rapid_speedup_count"),
    count(when(col("isRapidlySlowdown") == 1, 1)).alias("rapid_slowdown_count"),
    count(when(col("isFatigueDriving") == 1, 1)).alias("fatigue_count"),
    count(when(col("isHthrottleStop") == 1, 1)).alias("hthrottleStop_count"),
    count(when(col("isOilLeak") == 1, 1)).alias("oilLead_count")
)

data_chart = read_Files.select(
    "driverID", "carPlateNumber", "Latitude", "Longtitude", "Direction", "siteName", "Time", "Speed", "isOverspeed"
)

data_summary.coalesce(1).write.mode("overwrite").json(out + "summary")
data_chart.coalesce(1).write.mode("overwrite").partitionBy("driverID").json(out + "chartData")

spark.stop()