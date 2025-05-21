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

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

import utils.data_processing_bronze_table
import utils.data_processing_silver_table
import utils.data_processing_gold_table


# Initialize SparkSession
spark = pyspark.sql.SparkSession.builder \
    .appName("dev") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to ERROR to hide warnings
spark.sparkContext.setLogLevel("ERROR")

# set up config
snapshot_date_str = "2023-01-01"

start_date_str = "2023-01-01"
end_date_str = "2024-12-01"

# generate list of dates to process
def generate_first_of_month_dates(start_date_str, end_date_str):
    # Convert the date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # List to store the first of month dates
    first_of_month_dates = []

    # Start from the first of the month of the start_date
    current_date = datetime(start_date.year, start_date.month, 1)

    while current_date <= end_date:
        # Append the date in yyyy-mm-dd format
        first_of_month_dates.append(current_date.strftime("%Y-%m-%d"))
        
        # Move to the first of the next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)

    return first_of_month_dates

dates_str_lst = generate_first_of_month_dates(start_date_str, end_date_str)
# print(dates_str_lst)

# create bronze datalake
bronze_directory = "datamart/bronze/"

if not os.path.exists(bronze_directory):
    os.makedirs(bronze_directory)

# Store attribute data
date_str = "2023-01-01"
print(
    """
    -------------
    Copying attribute data to bronze datamart.
    -------------
    """
)
utils.data_processing_bronze_table.process_bronze_table(date_str, bronze_directory, spark, monthly = False)

# run bronze backfill
print(
    """
    -------------
    Copying transactional data to bronze datamart.
    -------------
    """
)
for date_str in dates_str_lst:
    utils.data_processing_bronze_table.process_bronze_table(date_str, bronze_directory, spark, monthly = True)


# create silver datalake
silver_directory = "datamart/silver/"

print(
    """
    -------------
    Copying data to silver datamart.
    -------------
    """
)

if not os.path.exists(silver_directory):
    os.makedirs(silver_directory)
utils.data_processing_silver_table.process_silver_table(date_str, bronze_directory, silver_directory, spark, transactional = False)
if not os.path.exists(silver_directory):
    os.makedirs(silver_directory)
utils.data_processing_silver_table.process_silver_table(date_str, bronze_directory, silver_directory, spark, transactional = False)

# run silver backfill
for date_str in dates_str_lst:
    utils.data_processing_silver_table.process_silver_table(date_str, bronze_directory, silver_directory, spark)


# create gold datalake
gold_directory = "datamart/gold/"

if not os.path.exists(gold_directory):
    os.makedirs(gold_directory)

# # run gold backfill
print(
    """
    -------------
    Creating feature and label stores in gold datamart.
    -------------
    """
)
for date_str in dates_str_lst:
    utils.data_processing_gold_table.process_labels_gold_table(date_str, silver_directory, gold_directory, spark, dpd = 30, mob = 6)
    utils.data_processing_gold_table.process_gold_table(date_str, silver_directory, gold_directory, spark, dpd = 30, mob = 6)

print(
    """
    -------------
    All files loaded into bronze, silver, and gold datamarts!
    -------------
    """
)



    