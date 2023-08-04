def analyze_tip_payment(df, gcs_output_path=None):
    tip_payment_type = df.groupBy("payment_type")\
                    .avg("tip_amount")\
                    .withColumnRenamed("avg(tip_amount)", "avg_tip_payment_type")\
                    .orderBy("avg_tip_payment_type", ascending=False)

    tip_payment_type.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{gcs_output_path}/tip_payment_type_analysis")