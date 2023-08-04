
from jobs.trip_analysis.duration_dist import analyse_duration_dist
from jobs.trip_analysis.top_locations import analyze_top_locations
 
def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    # 1. Average distance and duration of rides
    analyse_duration_dist(df, gcs_output_path)

    # 2. Find top 10 pick-up and drop-off locations
    analyze_top_locations(df, gcs_output_path)

    spark.stop()