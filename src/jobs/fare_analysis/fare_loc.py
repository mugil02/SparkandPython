

def analyze_fare_loc(df, gcs_output_path=None):
    pickup_fare_avg = df.groupBy("PULocationID").avg("fare_amount").withColumnRenamed("avg(fare_amount)", "avg_pickup_fare").orderBy("avg_pickup_fare", ascending=False)
    dropoff_fare_avg = df.groupBy("DOLocationID").avg("fare_amount").withColumnRenamed("avg(fare_amount)", "avg_dropoff_fare").orderBy("avg_dropoff_fare", ascending=False)

    pickup_fare_avg.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{gcs_output_path}/pickup_fare_avg_analysis")

    dropoff_fare_avg.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{gcs_output_path}/dropoff_fare_avg_analysis")

    