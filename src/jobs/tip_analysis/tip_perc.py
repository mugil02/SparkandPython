def analyze_tip_perc(df, gcs_output_path=None):

    pickup_tip_avg = df.groupBy("PULocationID").avg("tip_amount").withColumnRenamed("avg(tip_amount)", "avg_pickup_tip")
    dropoff_tip_avg = df.groupBy("DOLocationID").avg("tip_amount").withColumnRenamed("avg(tip_amount)", "avg_dropoff_tip")

    tip_analysis = pickup_tip_avg.join(dropoff_tip_avg, pickup_tip_avg.PULocationID == dropoff_tip_avg.DOLocationID, "outer") \
                                .select(
                                    pickup_tip_avg.PULocationID,
                                    pickup_tip_avg.avg_pickup_tip,
                                    dropoff_tip_avg.avg_dropoff_tip
                                )
    
    tip_analysis.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{gcs_output_path}/tip_analysis")
