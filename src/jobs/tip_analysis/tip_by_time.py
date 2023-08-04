from pyspark.sql.functions import avg, dayofmonth, year, dayofweek


def analyze_tip_time(df, gcs_output_path=None):
    # YEAR
    df_year_tip = df.groupBy(
        year("tpep_pickup_datetime").alias("year")).agg(
        avg("tip_amount").alias("avg_tip_amnt_year")
    ).orderBy("year", ascending=True)

    df_year_tip.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{gcs_output_path}/tip_year_analysis")

    # DAY OF THE MONTH
    df_day_tip = df.groupBy(
        dayofmonth("tpep_pickup_datetime").alias("day")).agg(
        avg("tip_amount").alias("avg_tip_amnt_day")
    ).orderBy("day", ascending=True) \

    df_day_tip.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{gcs_output_path}/tip_day_of_month_analysis")

    # DAY OF WEEK
    df_day_week_tip = df.groupBy(
        dayofweek("tpep_pickup_datetime").alias("weekday")).agg(
        avg("tip_amount").alias("avg_tip_amnt_weekday")
    ).orderBy("weekday", ascending=True) \

    df_day_week_tip.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{gcs_output_path}/tip_day_of_week_analysis")
    