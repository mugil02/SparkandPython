from jobs.fare_analysis.fare_dist_corr import analyse_fare_dist
from jobs.fare_analysis.fare_loc import analyze_fare_loc
from jobs.fare_analysis.fare_passenger_count import analyse_fare_pass_cnt
 

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    # 1. fare-loc
    analyze_fare_loc(df, gcs_output_path)

    # 2. fare and passenger count
    analyse_fare_pass_cnt(df, gcs_output_path)

    # 3. fare-distance
    analyse_fare_dist(df, gcs_output_path)

    spark.stop()