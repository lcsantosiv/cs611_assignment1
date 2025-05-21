import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
import pyspark.sql.functions as F
import argparse

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType
from pyspark.sql.window import Window


def process_gold_table(snapshot_date_str, silver_directory, gold_directory, spark, dpd, mob):
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    silver_directory_1 = silver_directory + "lms_loan_daily/"
     
    # Finalizing loan data for gold
    partition_name = "silver_loan_monthly_" + snapshot_date_str.replace('-','_') + '.parquet'
    filepath = silver_directory_1 + partition_name
    df = spark.read.parquet(filepath)
    print('loaded from:', filepath, 'row count:', df.count())
    
    df = df.where(F.col("mob") == mob)
    
    # Create binary flags for overdue amount and missed payment
    df = df.withColumn("has_overdue_amt", F.when(F.col("overdue_amt") > 0, 1).otherwise(0))
    df = df.withColumn("has_missed_payment", F.when(F.col("first_missed_date").isNotNull(), 1).otherwise(0))
    
    # Drop redundant fields
    df = df.drop("due_amt", "latest_snapshot_date", "loan_start_date", "paid_amt", "first_missed_date")
    
    # Get silver attributes
    attributes_df = spark.read.parquet("datamart/silver/features_attributes/silver_attributes_cleaned.parquet").drop("snapshot_date")
    
    # Join silver attributes to final_df
    df = df.join(attributes_df, on="Customer_ID", how="left")
    
    # Get silver financials
    financials_df = spark.read.parquet("datamart/silver/features_financials/silver_financials_cleaned.parquet").drop("credit_mix", "payment_behaviour","credit_history_years","credit_history_months", "snapshot_date")
    
    # Join silver financials to final_df
    df = df.join(financials_df, on="Customer_ID", how="left")
    
    # Get clickstream data
    silver_directory_2 = silver_directory + "feature_clickstream/"
    partition_name = "feature_clickstream_" + snapshot_date_str.replace('-','_') + '.parquet'
    filepath = silver_directory_2 + partition_name
    
    clickstream = spark.read.parquet(filepath).drop("snapshot_date")
    print('loaded from:', filepath, 'row count:', clickstream.count())

    # Join clickstream data and drop dpd and mob as these are used for labels
    df = df.join(clickstream, on = "Customer_ID", how = "left").drop("mob", "dpd")

    partition_name = "feature_store_" + snapshot_date_str.replace('-','_') + '.parquet'
    path = f"datamart/gold/feature_store/{partition_name}"
    df.write.mode("overwrite").parquet(path)
    print(f"Saved {path}. No of entries: {df.count()}.")

    return df

def process_labels_gold_table(snapshot_date_str, silver_directory, gold_directory, spark, dpd, mob):
    # prepare arguments
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    silver_directory = silver_directory + "lms_loan_daily/"
    # connect to silver table
    partition_name = "silver_loan_monthly_" + snapshot_date_str.replace('-','_') + '.parquet'
    filepath = silver_directory + partition_name
    df = spark.read.parquet(filepath)
    print('loaded from:', filepath, 'row count:', df.count())

    # get customer at mob
    df = df.filter(F.col("mob") == mob)

    # get label
    df = df.withColumn("label", F.when(F.col("dpd") >= dpd, 1).otherwise(0).cast(IntegerType()))
    df = df.withColumn("label_def", F.lit(str(dpd)+'dpd_'+str(mob)+'mob').cast(StringType()))

    # select columns to save
    df = df.select("loan_id", "Customer_ID", "label", "label_def", "snapshot_date")

    # save gold table - IRL connect to database to write
    partition_name = "gold_label_store_" + snapshot_date_str.replace('-','_') + '.parquet'
    path = f"datamart/gold/label_store/{partition_name}"
    df.write.mode("overwrite").parquet(path)
    print(f"Saved {path}. No of entries: {df.count()}.")
    
    return df
