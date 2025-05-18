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


def process_silver_table(snapshot_date_str, bronze_directory, silver_directory, spark, transactional = True):
    if transactional:
        print('transactional')
    else:
        # Silver processing for features_attributes
        file_name = "features_attributes/features_attributes.csv"
        filepath = bronze_directory + file_name
        df = spark.read.csv(filepath, header=True, inferSchema=True)
        
        # SSN is assumed to have xxx-xx-xxxx format
        ssn_pattern = r"^\d{3}-\d{2}-\d{4}$"
        df_clean = df.withColumn(
            "SSN",
            F.when(F.col("SSN").rlike(ssn_pattern), F.col("SSN")).otherwise(None)
        )
        
        # Create ssn_valid boolean
        df_clean = df_clean.withColumn("ssn_valid",F.when(F.col("SSN").isNull(), 0).otherwise(1))
        
        # Blank Occupations are cleaned
        invalid_occ = ["_______", "", None]
        df_clean = df_clean.withColumn("Occupation_clean", F.trim(F.col("Occupation"))) \
            .withColumn(
                "Occupation_clean",
                F.when(
                    F.col("Occupation_clean")
                    .isin(invalid_occ), 
                    None)
                .otherwise(F.col("Occupation_clean"))
            )
        
        # Remove white spaces on Name column
        df_clean = df_clean.withColumn("Name", F.trim(F.col("Name")))
        
        # Clean Age - assumed reasonable age is 15 to 100 years old
        df_clean = df_clean.withColumn("Age_int", F.col("Age").cast(IntegerType()))
        df_clean = df_clean.filter((F.col("Age_int") >= 15) & (F.col("Age_int") <= 100))
        
        df_clean = df.withColumn("ssn_valid", F.when(F.col("SSN").rlike(r"^\d{3}-\d{2}-\d{4}$"), 1).otherwise(0)) \
                     .withColumn("occupation_known", F.when(F.col("Occupation").isin("_______", "", None), 0).otherwise(1)) \
                     .withColumn("age_valid", F.when((F.col("Age").cast("int").isNotNull()) & (F.col("Age") >= 15), 1).otherwise(0))
        
        name_counts = df_clean.groupBy("Name").agg(F.count("*").alias("name_shared_count"))
        df_with_name_count = df_clean.join(name_counts, on="Name", how="left")
        df_with_name_count = df_with_name_count.withColumn("is_name_shared", F.when(F.col("name_shared_count") > 1, 1).otherwise(0))
        df_with_name_count = df_with_name_count.drop("Name", "SSN",)
        print('loaded from:', filepath, 'row count:', df_with_name_count.count())
        # save silver table - IRL connect to database to write
        file_name = "silver_attributes_cleaned.parquet"
        filepath = silver_directory + file_name
        df_with_name_count.write.mode("overwrite").parquet(filepath)
        # df_with_name_count.toPandas().to_parquet(filepath,
                  # compression='gzip')
        print('saved to:', filepath)
    
    # # prepare arguments
    # snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
    # # connect to bronze table
    # partition_name = "bronze_loan_daily_" + snapshot_date_str.replace('-','_') + '.csv'
    # filepath = bronze_directory + partition_name
    # df = spark.read.csv(filepath, header=True, inferSchema=True)
    # print('loaded from:', filepath, 'row count:', df.count())

    # # clean data: enforce schema / data type
    # # Dictionary specifying columns and their desired datatypes
    # column_type_map = {
    #     "loan_id": StringType(),
    #     "Customer_ID": StringType(),
    #     "loan_start_date": DateType(),
    #     "tenure": IntegerType(),
    #     "installment_num": IntegerType(),
    #     "loan_amt": FloatType(),
    #     "due_amt": FloatType(),
    #     "paid_amt": FloatType(),
    #     "overdue_amt": FloatType(),
    #     "balance": FloatType(),
    #     "snapshot_date": DateType(),
    # }

    # for column, new_type in column_type_map.items():
    #     df = df.withColumn(column, col(column).cast(new_type))

    # # augment data: add month on book
    # df = df.withColumn("mob", col("installment_num").cast(IntegerType()))

    # # augment data: add days past due
    # df = df.withColumn("installments_missed", F.ceil(col("overdue_amt") / col("due_amt")).cast(IntegerType())).fillna(0)
    # df = df.withColumn("first_missed_date", F.when(col("installments_missed") > 0, F.add_months(col("snapshot_date"), -1 * col("installments_missed"))).cast(DateType()))
    # df = df.withColumn("dpd", F.when(col("overdue_amt") > 0.0, F.datediff(col("snapshot_date"), col("first_missed_date"))).otherwise(0).cast(IntegerType()))

    # # save silver table - IRL connect to database to write
    # partition_name = "silver_loan_daily_" + snapshot_date_str.replace('-','_') + '.parquet'
    # filepath = silver_directory + partition_name
    # df.write.mode("overwrite").parquet(filepath)
    # # df.toPandas().to_parquet(filepath,
    # #           compression='gzip')
    # print('saved to:', filepath)
    
        return df_with_name_count