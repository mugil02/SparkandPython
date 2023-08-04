from pyspark.sql.functions import month, avg, year, dayofmonth, hour
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline


def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)
    df_features = df.withColumn("pickup_datetime", df["tpep_pickup_datetime"].cast("timestamp"))

    df_features = df_features.withColumn("pickup_hour", hour(df_features["pickup_datetime"]))
    df_features = df_features.withColumn("pickup_day_of_month", dayofmonth(df_features["pickup_datetime"]))
    df_features = df_features.withColumn("pickup_month", month(df_features["pickup_datetime"]))
    df_features = df_features.withColumn("pickup_year", year(df_features["pickup_datetime"]))

    df_features = df_features.drop(*["VendorID", "RatecodeID", "PULocationID", "DOLocationID", "pickup_datetime",
                                    "tpep_pickup_datetime", "dropoff_datetime", "tpep_dropoff_datetime"])
    
    aggregated_data = df_features.groupBy(
        "pickup_hour", "pickup_day_of_month", "pickup_month", "pickup_year"
    ).agg(
        sum("passenger_count").alias("number_of_pickups"),
        avg("trip_distance").alias("avg_trip_distance"),
        avg("fare_amount").alias("avg_fare_amount"),
        avg("total_amount").alias("avg_total_amount"),
    ).orderBy("pickup_hour", "pickup_day_of_month", "pickup_month", "pickup_year")

    aggregated_data.limit(10).toPandas()

    features = aggregated_data.columns
    features.remove("number_of_pickups")
    
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    data_assembled = assembler.transform(aggregated_data)

    train_data, test_data = data_assembled.randomSplit([0.8, 0.2], seed=42)

    lr = LinearRegression(featuresCol="features", labelCol="number_of_pickups",  regParam=0.2)
    pipeline = Pipeline(stages=[lr])
    model = pipeline.fit(train_data)

    predictions = model.transform(test_data)

    final_predicted_df = predictions.select('pickup_hour',
                                            'pickup_day_of_month',
                                            'pickup_month',
                                            'pickup_year',"number_of_pickups", "prediction")
    
    final_predicted_df.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{gcs_output_path}/prediction_analysis")

    spark.stop()