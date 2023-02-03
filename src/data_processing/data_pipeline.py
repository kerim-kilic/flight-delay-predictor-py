from pyspark.sql.functions import *
from pyspark.ml.feature import *

def process_data(raw_data, origin_weather_data, destination_weather_data):
    temp1 = raw_data.withColumn("planned_arrival_time", to_timestamp("planned_arrival_time")) \
        .withColumn("planned_departure_time", to_timestamp("planned_departure_time")) \
        .withColumn("actual_arrival_time", to_timestamp("actual_arrival_time")) \
        .withColumn("delay_time", col("planned_arrival_time").cast("long") - col("actual_arrival_time").cast("long")) \
        .withColumn("delay_time", col("delay_time") / 60) \
        .withColumn("date", concat_ws("-", col("year"), col("month"), col("day_of_month"))) \
        .withColumn("date", to_date("date"))

    df = temp1.withColumn("flight_time",
                          col("planned_arrival_time").cast("long") - col("planned_departure_time").cast("long")) \
        .withColumn("flight_time", col("flight_time") / 60) \
        .withColumn("speed", col("flight_distance").cast("long") / col("flight_time").cast("long"))

    df = df.withColumn('delay', when(df["delay_time"] >= 15, "1").otherwise("0")).na.drop("any")

    df = df[
        df["origin"].isin(["ATL", "DEN", "DFW", "LAS", "LAX", "MSP", "ORD", "PHX", "SEA", "SFO"])
        & df["destination"].isin(["ATL", "DEN", "DFW", "LAS", "LAX", "MSP", "ORD", "PHX", "SEA", "SFO"])
        ]

    df = df.select(
        "date",
        "quarter",
        "month",
        "day_of_month",
        "day_of_week",
        "flight_distance",
        "seating_capacity",
        "origin",
        "destination",
        "delay",
        "flight_time",
        "carrier",
        "speed",
        "planned_departure_local_hour",
        "planned_arrival_local_hour"
    )

    columns = ["quarter", "month", "day_of_month", "day_of_week", "flight_distance", "seating_capacity",
               "planned_departure_local_hour", "planned_arrival_local_hour"]
    for column_x in columns:
        df = df.withColumn(column_x, col(column_x).cast("int"))

    # Code for one-hot-encoding
    one_hot_features = ["origin", "destination", "carrier"]
    one_hot_features_ind = []
    one_hot_features_out = []

    for item in one_hot_features:
        one_hot_features_ind.append(item + "_ind")
        one_hot_features_out.append(item + "_out")

    SI = StringIndexer().setInputCols(one_hot_features).setOutputCols(one_hot_features_ind)
    df = SI.fit(df).transform(df)
    ohe = OneHotEncoder().setInputCols(one_hot_features_ind).setOutputCols(one_hot_features_out)
    # df = ohe.fit(df).transform(df).drop(*one_hot_features)
    df = ohe.fit(df).transform(df)

    # Code for weather data
    origin_weather_data = origin_weather_data \
        .withColumn("date", to_date("date")) \
        .withColumn("origin_precip", col("origin_precip").cast("double")) \
        .withColumn("origin_total_snow", col("origin_total_snow").cast("double"))

    destination_weather_data = destination_weather_data \
        .withColumn("date", to_date("date")) \
        .withColumn("destination_precip", col("destination_precip").cast("double")) \
        .withColumn("destination_total_snow", col("destination_total_snow").cast("double"))

    columns = ["origin_wd", "origin_ws", "origin_visibility", "origin_cloudcover", "origin_atmos_pressure",
               "origin_air_temp", "origin_dew_point", "origin_wind_gust"]
    for column_x in columns:
        origin_weather_data = origin_weather_data.withColumn(column_x, col(column_x).cast("int"))

    columns = ["destination_wd", "destination_ws", "destination_visibility", "destination_cloudcover",
               "destination_atmos_pressure", "destination_air_temp", "destination_dew_point", "destination_wind_gust"]
    for column_x in columns:
        destination_weather_data = destination_weather_data.withColumn(column_x, col(column_x).cast("int"))

    df = df.join(origin_weather_data, ["origin", "date"]) \
        .join(destination_weather_data, ["destination", "date"])

    # Code for airport congestion
    total_departure_flights = temp1. \
        groupby(["origin", "date"]). \
        count(). \
        selectExpr("origin as origin", "date as date", "count as total_departure_flights")
    total_arrival_flights = temp1. \
        groupby(["destination", "date"]). \
        count(). \
        selectExpr("destination as destination", "date as date", "count as total_arrival_flights")
    departing_carrier_flights = temp1. \
        groupby(["origin", "carrier", "date"]). \
        count(). \
        selectExpr("origin as origin", "carrier as carrier", "date as date", "count as departing_carrier_flights")

    df = df.join(total_departure_flights, ["origin", "date"]) \
        .join(total_arrival_flights, ["destination", "date"]) \
        .join(departing_carrier_flights, ["origin", "carrier", "date"]) \
        .drop(*one_hot_features, "date")
    return df