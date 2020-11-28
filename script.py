# Import libraries
import pyspark
from pyspark.sql import SQLContext, window
from pyspark.sql import SparkSession

# Create spark context
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Data_Wrangling').getOrCreate()
sc = spark.sparkContext
sql = SQLContext(sc)

# Create dataframes
df_detectors = (sql.read
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", True)
                .load("data/freeway_detectors.csv"))

df_loopdata = (sql.read
               .format("com.databricks.spark.csv")
               .option("header", "true")
               .option("inferSchema", True)
               .option("timestampFormat", "yyyy-MM-dd HH:mm:ssx")
               .load("data/freeway_loopdata.csv"))

df_stations = (sql.read
               .format("com.databricks.spark.csv")
               .option("header", "true")
               .option("inferSchema", True)
               .load("data/freeway_stations.csv"))

df_highways = (sql.read
               .format("com.databricks.spark.csv")
               .option("header", "true")
               .option("inferSchema", True)
               .load("data/highways.csv"))

df_readings = df_loopdata.join(df_detectors, on=['detectorid'], how='full')
df_readings = df_readings.fillna(0)

df_readings = df_readings.select(df_readings.stationid,
                                 df_readings.volume, df_readings.speed,
                                 ).groupBy(
    df_readings.stationid, window(df_readings.starttime, "5 minutes").alias(
        "time_window")) \
    .agg(sum(when(df_readings.volume.isNotNull(),
                  df_readings.volume).otherwise(lit(0))).alias("total_volume"),
         sum(df_readings.speed * df_readings.volume).alias("total_speed"),
         sum(when(df_readings.speed > 80, df_readings.volume).otherwise(lit(
             0))).alias(
             "Speed>80"),
         sum(when((df_readings.speed < 5) & (df_readings.speed > 0),
                  df_readings.volume).otherwise(lit(0))).alias(
             "speed<5"))

df_readings = df_readings.select(df_readings.stationid,
                                 df_readings.time_window.start.alias(
                                     "startdate"),
                                 df_readings.time_window.end.alias("enddate"),
                                 df_readings.total_volume,
                                 df_readings.total_speed,
                                 df_readings["speed>80"],
                                 df_readings["speed<5"])

df_readings.show(20, False)

df_metadata = df_highways.join(df_stations, on=['highwayid'], how='full')
df_metadata = df_metadata.select(df_metadata.stationid,
                                 df_metadata.locationtext,
                                 df_metadata.milepost,
                                 df_metadata.length,
                                 df_metadata.direction,
                                 df_metadata.upstream,
                                 df_metadata.downstream)

df_metadata.show()

# Convert to CSV
df_readings.coalesce(1).write.option("header", "true").csv("aggregated.csv")
df_metadata.coalesce(1).write.option("header", "true").csv("metadata.csv")
