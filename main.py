from pyspark.sql import *
from pyspark.ml.classification import *
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
from pyspark.sql.functions import *
from pyspark.ml.feature import *
from pyspark.pandas import *
from src.data_pipeline import *
from src.data_split import *

from pysparkling import *
import h2o

# Create Spark session
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("test") \
      .getOrCreate()

# h2oContext = H2OContext.getOrCreate(spark)

### Read in the data
raw_data = spark.read.csv("data/flights_2017.csv", header=True)
origin_weather_data = spark.read.csv("data/origin_weather_data.csv", header=True)
destination_weather_data = spark.read.csv("data/destination_weather_data.csv", header=True)

df = process_data(raw_data, origin_weather_data, destination_weather_data)

# Split data in training and testing data
splits = split_data(data=df,
                    ratio=0.9,
                    seed=420)

train_data = splits[0]
test_data = splits[1]

spark.stop()