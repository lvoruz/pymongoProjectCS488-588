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
df_freeway_detectors = (sql.read
                        .format("com.databricks.spark.csv")
                        .option("header", "true")
                        .option("inferSchema", True)
                        .load("freeway_detectors.csv"))

df_freeway_loopdata = (sql.read
                       .format("com.databricks.spark.csv")
                       .option("header", "true")
                       .option("inferSchema", True)
                       .load("freeway_loopdata.csv"))

df_freeway_stations = (sql.read
                       .format("com.databricks.spark.csv")
                       .option("header", "true")
                       .option("inferSchema", True)
                       .load("freeway_stations.csv"))

df_highways = (sql.read
               .format("com.databricks.spark.csv")
               .option("header", "true")
               .option("inferSchema", True)
               .load("highways.csv"))

# Drop unwanted columns
df_detector = df_detector.drop("milepost", "detectortitle", "lanenumber",
                               "agency_lane", "active_dates", "enabledflag",
                               "detectortype", "detectorclass",
                               "detectorstatus", "rampid", "controllerid",
                               "start_date", "end_date", "atms_id",
                               "active_dates", "locationtext")

df_station = df_station.drop("milepost", "length", "numberlanes", "agencyid",
                             "active_dates", "numberlanes", "length_mid",
                             "downstream_mile", "upstream_mile", "agencyid",
                             "opposite_stationid", "segment_geom",
                             "station_geom", "start_date", "end_date",
                             "detectortype", "agency", "region", "active_dates",
                             "id", "station_location_id", "detectorlocation",
                             "upstream", "downstream")

# Rename a column so that the joins will match on that column
df_detector = df_detector.withColumnRenamed('detectorid', 'detector_id')

# Join the dataframes
df_first = df_highway.join(df_detector, on=['detector_id'], how='full')

# Drop highwayid to avoid duplicate
df_first = df_first.drop("highwayid")

# Join to get the final result
df = df_first.join(df_station, on=['stationid'], how='full')

# Changes by Brian
df = df.select(df.starttime, to_timestamp(df.starttime).alias(
    'timestamp'), df.volume).where(df.starttime.isNotNull())
df.printSchema()
df.groupBy(df.starttime).agg(sum(df.volume)).show()
df.show()

# Show the result
# df.show()

# Convert to CSV
# df.coalesce(1).write.option("header", "true").csv("processed_data.csv")
