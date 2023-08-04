
from jobs.tip_analysis.tip_by_time import analyze_tip_time
from jobs.tip_analysis.tip_payment_type import analyze_tip_payment
from jobs.tip_analysis.tip_perc import analyze_tip_perc
 
def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    # 1. Tip percentage for locations
    analyze_tip_perc(df, gcs_output_path)

    # 2. tip during different time
    analyze_tip_time(df, gcs_output_path)

    # 3. tip-payment 
    analyze_tip_payment(df, gcs_output_path)

    spark.stop()