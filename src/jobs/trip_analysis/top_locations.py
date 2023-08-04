

def analyze_top_locations(df, gcs_output_path=None):

    pickup_counts = df.groupBy("PULocationID").count()\
                        .withColumnRenamed("count", "pickup_count")\
                        .orderBy("pickup_count", ascending=False)\
                        .limit(10)
    
    dropoff_counts = df.groupBy("DOLocationID").count()\
                        .withColumnRenamed("count", "dropoff_count")\
                        .orderBy("dropoff_count", ascending=False)\
                        .limit(10)
    
    pickup_counts.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{gcs_output_path}/top_pickup_analysis")

    dropoff_counts.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{gcs_output_path}/top_dropoff_analysis")
