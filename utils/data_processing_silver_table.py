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
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType, StructField, StructType


def process_silver_table(snapshot_date_str, bronze_directory, silver_directory, spark, transactional = True):
    if transactional:
        # Processing tables for feature_clickstream
        bronze_directory_1 = bronze_directory + 'feature_clickstream/'
        silver_directory_1 = silver_directory + 'feature_clickstream/'
        
        snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
        partition_name = "feature_clickstream_" + snapshot_date_str.replace('-','_') + '.csv'
        
        filepath = bronze_directory_1 + partition_name        
        df_raw = spark.read.csv(filepath, header=True, inferSchema=True)
        print('loaded from:', filepath, 'row count:', df_raw.count())
        
        df_clean = df_raw.select(
            *[F.col(f"fe_{i}").cast(IntegerType()) for i in range(1, 21)],
            F.col("Customer_ID").cast(StringType()).alias("Customer_ID"),
            F.to_date(F.col("snapshot_date")).alias("snapshot_date")
        )
        
        df_clean = df_clean.na.drop(subset=["Customer_ID", "snapshot_date"])
        schema = StructType(
        [StructField(f"fe_{i}", IntegerType(), True) for i in range(1, 21)] +
        [StructField("Customer_ID", StringType(), False),
         StructField("snapshot_date", DateType(), False)]
        )
        df_clean = spark.createDataFrame(df_clean.rdd, schema=schema)
        snapshot_date_str = df_clean.select(F.date_format("snapshot_date", "yyyy_MM_dd")) \
                                   .limit(1).collect()[0][0]
        
        parquet_filename = f"feature_clickstream_{snapshot_date_str}.parquet"
        parquet_path = os.path.join(silver_directory_1, parquet_filename)
        
        df_clean.write.mode("overwrite").parquet(parquet_path)       
        print(f"Saved {parquet_filename}")

        # Processing tables for lms
        bronze_directory_2 = bronze_directory + 'lms_loan_daily/'
        silver_directory_2 = silver_directory + 'lms_loan_daily/'

        partition_name = "lms_loan_daily_" + snapshot_date_str.replace('-','_') + '.csv'
        filepath = bronze_directory_2 + partition_name
        df = spark.read.csv(filepath, header=True, inferSchema=True)
        print('loaded from:', filepath, 'row count:', df.count())
    
        # clean data: enforce schema / data type
        # Dictionary specifying columns and their desired datatypes
        column_type_map = {
            "loan_id": StringType(),
            "Customer_ID": StringType(),
            "loan_start_date": DateType(),
            "tenure": IntegerType(),
            "installment_num": IntegerType(),
            "loan_amt": FloatType(),
            "due_amt": FloatType(),
            "paid_amt": FloatType(),
            "overdue_amt": FloatType(),
            "balance": FloatType(),
            "snapshot_date": DateType(),
        }
    
        for column, new_type in column_type_map.items():
            df = df.withColumn(column, F.col(column).cast(new_type))
    
        # augment data: add month on book
        df = df.withColumn("mob", F.col("installment_num").cast(IntegerType()))
    
        # augment data: add days past due
        df = df.withColumn("installments_missed", F.ceil(F.col("overdue_amt") / F.col("due_amt")).cast(IntegerType())).fillna(0)
        df = df.withColumn("first_missed_date", F.when(F.col("installments_missed") > 0, F.add_months(F.col("snapshot_date"), -1 * F.col("installments_missed"))).cast(DateType()))
        df = df.withColumn("dpd", F.when(F.col("overdue_amt") > 0.0, F.datediff(F.col("snapshot_date"), F.col("first_missed_date"))).otherwise(0).cast(IntegerType()))
    
        # save silver table - IRL connect to database to write
        partition_name = "silver_loan_monthly_" + snapshot_date_str.replace('-','_') + '.parquet'
        filepath = silver_directory_2 + partition_name
        df.write.mode("overwrite").parquet(filepath)
        print('saved to:', filepath)
        
    else:
        
        # Silver processing for features_attributes
        print('Processing silver table for features_attributes')
        file_name = "features_attributes/features_attributes.csv"
        filepath = bronze_directory + file_name
        df = spark.read.csv(filepath, header=True, inferSchema=True)
              
        # Blank Occupations are cleaned
        invalid_occ = ["_______", "", None]
        df_clean = df.withColumn("Occupation", F.trim(F.col("Occupation"))) \
            .withColumn("Occupation", F.when(F.col("Occupation").isin(invalid_occ), None).otherwise(F.col("Occupation")))
        
        # Remove white spaces on Name column
        df_clean = df_clean.withColumn("Name", F.trim(F.col("Name")))    

        df_clean = df_clean.withColumn("ssn_valid", F.when(F.col("SSN").rlike(r"^\d{3}-\d{2}-\d{4}$"), 1).otherwise(0)) \
                     .withColumn("occupation_known", F.when(F.col("Occupation").isNull(), 0).otherwise(1)) \
                     .withColumn("age_valid", F.when((F.col("Age").cast("int").isNotNull()) & (F.col("Age") >= 15), 1).otherwise(0))

        # Clean Age - assumed reasonable age is 15 to 100 years old
        df_clean = df_clean.withColumn("Age", F.col("Age").cast(IntegerType()))
        
        name_counts = df_clean.groupBy("Name").agg(F.count("*").alias("name_shared_count"))
        df_with_name_count = df_clean.join(name_counts, on="Name", how="left")
        df_with_name_count = df_with_name_count.withColumn("is_name_shared", F.when(F.col("name_shared_count") > 1, 1).otherwise(0))
        df_with_name_count = df_with_name_count.drop("Name", "SSN",)
        print('loaded from:', filepath, 'row count:', df_with_name_count.count())
        
        # save silver table - IRL connect to database to write
        file_name = "features_attributes/silver_attributes_cleaned.parquet"
        filepath = silver_directory + file_name
        df_with_name_count.write.mode("overwrite").parquet(filepath)
        print('saved to:', filepath)

        # Silver processing for features_financials
        print('Processing silver table for features_financials')
        bronze_directory = "datamart/bronze/"
        file_name = "features_financials/features_financials.csv"
        filepath = bronze_directory + file_name
        
        df = spark.read.csv(filepath, header=True, inferSchema=True)
        print('loaded from:', filepath, 'row count:', df.count())
        
        # Remove non-numeric characters from Annual_Income and cast
        df = df.withColumn('Annual_Income', F.regexp_replace('Annual_Income', '[^0-9.]', '').cast(FloatType()))
        
        # Cast numeric columns explicitly
        numeric_cols = ['Monthly_Inhand_Salary', 'Interest_Rate', 'Delay_from_due_date', 'Num_of_Delayed_Payment', 
                        'Changed_Credit_Limit', 'Num_Credit_Inquiries', 'Outstanding_Debt', 
                        'Credit_Utilization_Ratio', 'Total_EMI_per_month', 'Amount_invested_monthly', 'Monthly_Balance']
        for col in numeric_cols:
            df = df.withColumn(col, F.col(col).cast(FloatType()))
        
        count_cols = ['Num_Bank_Accounts', 'Num_Credit_Card', 'Num_of_Loan']
        for col in count_cols:
            df = df.withColumn(col, F.col(col).cast(IntegerType()))
        
        # Convert Payment_of_Min_Amount to boolean/int
        df = df.withColumn('Payment_of_Min_Amount', F.when(F.col('Payment_of_Min_Amount') == 'Yes', 1).otherwise(0))
        
        # Parse Credit_History_Age to total months
        df = df.withColumn(
            'Credit_History_Years', F.regexp_extract('Credit_History_Age', r'(\d+) Years', 1).cast(IntegerType())
        ).withColumn(
            'Credit_History_Months', F.regexp_extract('Credit_History_Age', r'(\d+) Months', 1).cast(IntegerType())
        ).withColumn(
            'Credit_History_Total_Months', F.col('Credit_History_Years') * 12 + F.coalesce(F.col('Credit_History_Months'), F.lit(0))
        )
        
        # Loan Types
        loan_types = (
            df
            .filter(F.col("Type_of_Loan").isNotNull())
            .select(F.explode(F.split(F.col("Type_of_Loan"), ",\\s*")).alias("Loan"))
            .select(
                F.trim(F.regexp_replace(F.col("Loan"), ",", "")).alias("Loan")  # remove commas inside the loan string
            )
            .filter(~F.col("Loan").rlike("(?i)^and\\b.*"))  # exclude entries starting with 'and'
            .distinct()
            .orderBy("Loan")
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        
        for loan in loan_types:
            col_name = loan.replace(" ", "_").replace("-", "_").lower()  # safe column name
            df = df.withColumn(
                col_name,
                F.when(F.col("Type_of_Loan").contains(loan), F.lit(1)).otherwise(F.lit(0))
            )
        
        # Handle missing/unknown in Credit_Mix
        df = df.withColumn('Credit_Mix', F.when(F.col('Credit_Mix') == '_', None).otherwise(F.col('Credit_Mix')))
        
        # One Hot Encoding for Credit_Mix
        df = df.withColumn("credit_mix_good",     F.when(F.col("Credit_Mix") == "Good", 1).otherwise(0)) \
                .withColumn("credit_mix_bad",      F.when(F.col("Credit_Mix") == "Bad", 1).otherwise(0)) \
                .withColumn("credit_mix_standard", F.when(F.col("Credit_Mix") == "Standard", 1).otherwise(0)) \
                .withColumn("valid_credit_mix",    F.when(F.col("Credit_Mix").isin(['Good','Standard','Bad']), 1).otherwise(0))
        
        # Handle Type_of_Loan NULL or Not Specified
        df = df.withColumn('Type_of_Loan', F.when(F.col('Type_of_Loan').isin(['NULL', 'Not Specified']), None).otherwise(F.col('Type_of_Loan')))
        
        # Payment Behaviour
        df = df.withColumn("Payment_Behaviour", F.lower(F.col("Payment_Behaviour")))        
        df = df.withColumn("Payment_Behaviour", F.when(F.col("Payment_Behaviour") == "!@9#%8", 0).otherwise(F.col("Payment_Behaviour")))        
        df = df.withColumn("has_valid_payment_behavior", F.col("Payment_Behaviour").isNotNull().cast("int"))
            
        payment_behaviours = (df.select("Payment_Behaviour").filter(F.col("Payment_Behaviour").isNotNull()).distinct().orderBy("Payment_Behaviour").rdd.flatMap(lambda x: x).collect())
        
        for behaviour in payment_behaviours:
            df = df.withColumn(f"pb_{behaviour}", (F.col("Payment_Behaviour") == behaviour).cast("int"))
        
        df = df.toDF(*[col.lower() for col in df.columns])
        df = df.drop("credit_history_age","type_of_loan")
        
        file_name = "features_financials/silver_financials_cleaned.parquet"
        filepath = silver_directory + file_name
        df.write.mode("overwrite").parquet(filepath)
        print('saved to:', filepath)
       
        return df_with_name_count