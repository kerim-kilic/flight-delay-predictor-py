from pyspark.sql import *
from src.data_pipeline import *
from src.data_split import *

# Create Spark session
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("test") \
      .getOrCreate()

### Read in the data
raw_data = spark.read.csv("/home/data/flights_2017.csv", header=True)
origin_weather_data = spark.read.csv("/home/data/origin_weather_data.csv", header=True)
destination_weather_data = spark.read.csv("/home/data/destination_weather_data.csv", header=True)

df = process_data(raw_data, origin_weather_data, destination_weather_data)

# Split data in training and testing data
splits = split_data(data=df,
                    ratio=0.9,
                    seed=420)

train_data = splits[0]
test_data = splits[1]

spark.stop()
print("Finished running script.")