
def analyse_fare_pass_cnt(df, gcs_output_path=None):
    passengerCnt_fare = df.groupBy("passenger_count").avg("fare_amount").withColumnRenamed("avg(fare_amount)", "passengerCnt_fare").orderBy("passengerCnt_fare", ascending=False)

    passengerCnt_fare.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{gcs_output_path}/passengerCnt_fare_analysis")