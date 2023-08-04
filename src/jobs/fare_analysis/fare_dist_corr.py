
def analyse_fare_dist(df, gcs_output_path=None):
    dist_fare = df.groupBy("trip_distance").avg("fare_amount").withColumnRenamed("avg(fare_amount)", "dist_fare").orderBy("dist_fare", ascending=False)

    dist_fare.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{gcs_output_path}/dist_fare_analysis")