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
                .load("freeway_detectors.csv"))

df_loopdata = (sql.read
               .format("com.databricks.spark.csv")
               .option("header", "true")
               .option("inferSchema", True)
               .option("timestampFormat", "yyyy-MM-dd HH:mm:ssx")
               .load("freeway_loopdata.csv"))

df_stations = (sql.read
               .format("com.databricks.spark.csv")
               .option("header", "true")
               .option("inferSchema", True)
               .load("freeway_stations.csv"))

df_highways = (sql.read
               .format("com.databricks.spark.csv")
               .option("header", "true")
               .option("inferSchema", True)
               .load("highways.csv"))

df_detectors.printSchema()
df_loopdata.printSchema()
df_stations.printSchema()
df_highways.printSchema()

df_readings = df_loopdata.join(df_detectors, on=['detectorid'], how='full')

df_readings = df_readings.select(df_readings.stationid,
                                 df_loopdata.volume, df_loopdata.speed).groupBy(
    df_readings.stationid, window(df_readings.starttime, "5 minutes").alias(
        "time_window")) \
    .agg(sum(df_loopdata.volume).alias("total_volume"),
         sum(when(df_loopdata.speed.isNotNull(),
                  df_loopdata.speed).otherwise(lit(0))).alias("speed"))

#df_readings = df_readings.select(df_readings.time_window,
#                                 df_readings.stationid,
#                                 df_readings.total_volume,
#                                 df_readings.total_speed).withColumn(
#    "total_speed", (df_readings.total_volume * df_readings.total_speed))


# SELECT WHERE > 80
# SELECT WHERE < 5
# Join with readings

df_readings.show(5, False)

# Join the dataframes

# Drop highwayid to avoid duplicate
# df_first = df_first.drop("highwayid")

# Join to get the final result
# df = df_first.join(df_station, on=['stationid'], how='full')

# Changes by Brian
# df.printSchema()
# df.groupBy(df.starttime).agg(sum(df.volume)).show()
# df.show()

# Show the result
# df.show()

# Convert to CSV
# df.coalesce(1).write.option("header", "true").csv("processed_data.csv")
