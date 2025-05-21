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

    # Finalizing loan data for gold
    df = spark.read.parquet("datamart/silver/lms_loan_daily/*.parquet")
    
    loan_aggregates = df.groupBy("loan_id").agg(
        F.sum("paid_amt").alias("total_paid_amt"),
        F.sum(F.when(F.col("overdue_amt") > 0, 1).otherwise(0)).alias("num_months_overdue"),
        F.max("snapshot_date").alias("latest_snapshot_date")
    )
    
    w = Window.partitionBy("loan_id").orderBy(F.col("snapshot_date").desc())
    # Add row number to identify latest per loan_id
    df = df.withColumn("row_num", F.row_number().over(w)) \
                     .filter(F.col("row_num") == 1) \
                     .drop("row_num")
    
    loan_df = df.join(loan_aggregates, on="loan_id", how="left")
    
    # Get features based on snapshot_date
    latest_snapshot = loan_df.groupBy("loan_id").agg(
        F.max("snapshot_date").alias("latest_snapshot_date"),
        F.first("loan_start_date").alias("loan_start_date")  # should be same for all rows
    )
    
    loan_start_features = latest_snapshot.select(
        "loan_id",
        "loan_start_date",
        "latest_snapshot_date",
        
        F.when(F.months_between("latest_snapshot_date", "loan_start_date") < 3, "new")
         .when(F.months_between("latest_snapshot_date", "loan_start_date") < 6, "mid")
         .otherwise("old").alias("loan_vintage"),
    
        F.month("loan_start_date").alias("loan_start_month"),
        F.year("loan_start_date").alias("loan_start_year")
    )
    
    final_loan_df = loan_df.join(loan_start_features, on="loan_id", how="left")
    
    # One Hot for loan vintage
    final_df = final_loan_df.withColumn("loan_vintage_old", F.when(F.col("loan_vintage") == "old", 1).otherwise(0)) \
                            .withColumn("loan_vintage_mid", F.when(F.col("loan_vintage") == "mid", 1).otherwise(0)) \
                            .withColumn("loan_vintage_new", F.when(F.col("loan_vintage") == "new", 1).otherwise(0))
    
    # Create binary flags for overdue amount and missed payment
    final_df = final_df.withColumn("has_overdue_amt", F.when(F.col("overdue_amt") > 0, 1).otherwise(0))
    final_df = final_df.withColumn("has_missed_payment", F.when(F.col("first_missed_date").isNotNull(), 1).otherwise(0))
    
    # Drop redundant fields
    final_df = final_df.drop("due_amt", "latest_snapshot_date", "loan_start_date", "paid_amt", "loan_vintage", "first_missed_date")

    # Get silver attributes
    attributes_df = spark.read.parquet("datamart/silver/features_attributes/silver_attributes_cleaned.parquet").drop("snapshot_date")

    # Join silver attributes to final_df
    final_df = final_df.join(attributes_df, on="Customer_ID", how="left")

    # Get silver financials
    financials_df = spark.read.parquet("datamart/silver/features_financials/silver_financials_cleaned.parquet").drop("credit_mix", "payment_behaviour","credit_history_years","credit_history_months", "snapshot_date")

    # Join silver financials to final_df
    final_df = final_df.join(financials_df, on="Customer_ID", how="left")

    # Get clickstream data
    df = spark.read.parquet("datamart/silver/feature_clickstream/*.parquet")

    # List of feature columns
    features = [f"fe_{i}" for i in range(1, 21)]
    
    # Aggregate: mean, stddev, min, max per customer
    agg_exprs = []
    for feat in features:
        agg_exprs += [
            F.mean(feat).alias(f"{feat}_mean"),
            F.stddev(feat).alias(f"{feat}_std"),
            F.min(feat).alias(f"{feat}_min"),
            F.max(feat).alias(f"{feat}_max"),
        ]
    
    agg_df = df.groupBy("Customer_ID").agg(*agg_exprs)
    
    # Create window to get first and last values per customer ordered by snapshot_date
    w = Window.partitionBy("Customer_ID").orderBy("snapshot_date")
    w_desc = Window.partitionBy("Customer_ID").orderBy(F.col("snapshot_date").desc())
    
    # Add first and last values of each feature
    for feat in features:
        df = df.withColumn(f"{feat}_first", F.first(feat).over(w))
        df = df.withColumn(f"{feat}_last", F.first(feat).over(w_desc))
        df = df.withColumn(f"{feat}_delta", F.col(f"{feat}_last") - F.col(f"{feat}_first"))
    
    # # Select only the last row per customer (to avoid duplicates in the deltas)
    df_deltas = df.withColumn("rn", F.row_number().over(w_desc)) \
                  .filter(F.col("rn") == 1) \
                  .select("Customer_ID", *[f"{feat}_delta" for feat in features])
    
    clickstreams = agg_df.join(df_deltas, on = "Customer_ID", how = "left")
    clickstreams = clickstreams.drop("snapshot_date")
    all_features = clickstreams.join(final_df, on = "Customer_ID", how = "left")
    # partition_name = "gold_feature_store_" + snapshot_date_str.replace('-','_') + '.parquet'
    # filepath = gold_label_store_directory + partition_name
    # df.write.mode("overwrite").parquet(filepath)

    # unique_dates = (
    #     all_features
    #     .select("snapshot_date")
    #     .distinct()
    #     .orderBy("snapshot_date")
    #     .rdd
    #     .map(lambda row: row["snapshot_date"])
    #     .collect()
    # )
    
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    # Generate the list of first-of-month dates
    unique_dates = []
    current = start_date
    
    while current <= end_date:
        unique_dates.append(current.strftime('%Y-%m-%d'))  # format as string (optional)
        # Move to the first of the next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    for date in unique_dates:
        df = all_features.filter(F.col("snapshot_date") == date)
        formatted_date = str(date).replace('-','_')
        path = f"datamart/gold/feature_store/feature_store_gold_{formatted_date}.parquet"
        df.write.mode("overwrite").parquet(path)
        print(f"Saved {path}")

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
    print(f"Saved {path}")
    
    return df
    
    # # save gold table - IRL connect to database to write
    # partition_name = "gold_feature_store_" + snapshot_date_str.replace('-','_') + '.parquet'
    # filepath = gold_label_store_directory + partition_name
    # df.write.mode("overwrite").parquet(filepath)
    # print('saved to:', filepath)


    # # prepare arguments
    # snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
    # # connect to bronze table
    # partition_name = "silver_loan_daily_" + snapshot_date_str.replace('-','_') + '.parquet'
    # filepath = silver_loan_daily_directory + partition_name
    # df = spark.read.parquet(filepath)
    # print('loaded from:', filepath, 'row count:', df.count())

    # # get customer at mob
    # df = df.filter(col("mob") == mob)

    # # get label
    # df = df.withColumn("label", F.when(col("dpd") >= dpd, 1).otherwise(0).cast(IntegerType()))
    # df = df.withColumn("label_def", F.lit(str(dpd)+'dpd_'+str(mob)+'mob').cast(StringType()))

    # # select columns to save
    # df = df.select("loan_id", "Customer_ID", "label", "label_def", "snapshot_date")

    # # save gold table - IRL connect to database to write
    # partition_name = "gold_label_store_" + snapshot_date_str.replace('-','_') + '.parquet'
    # filepath = gold_label_store_directory + partition_name
    # df.write.mode("overwrite").parquet(filepath)
    # # df.toPandas().to_parquet(filepath,
    # #           compression='gzip')
    # print('saved to:', filepath)
    
    return df