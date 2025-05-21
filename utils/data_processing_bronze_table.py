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


def process_bronze_table(snapshot_date_str, bronze_directory, spark, monthly = True):
    # prepare arguments
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
    # connect to source back end - IRL connect to back end source system
    folder_path = "data"
    # csv_files = [f[:-4] for f in os.listdir(folder_path) if f.endswith('.csv')]

    if monthly == True:
        csv_files = ['feature_clickstream','lms_loan_daily']
        for csv in csv_files:
            csv_file_path = f"data/{csv}.csv"
            # load data - IRL ingest from back end source system
            df = spark.read.csv(csv_file_path, header=True, inferSchema=True).filter(col('snapshot_date') == snapshot_date_str)
            print(snapshot_date_str + 'row count: ', df.count())
    
            # save bronze table to datamart - IRL connect to database to write
            if not os.path.exists(bronze_directory + f"/{csv}"):
                os.makedirs(bronze_directory + f"/{csv}")
            partition_name = f"{csv}/{csv}_" + snapshot_date_str.replace('-','_') + '.csv'
            filepath = bronze_directory + partition_name
            df.toPandas().to_csv(filepath, index=False)
            print('saved to:', filepath)

    else:
        csv_files = ['features_attributes','features_financials']
        for csv in csv_files:
            csv_file_path = f"data/{csv}.csv"
            df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
            
            
            if not os.path.exists(bronze_directory + f"/{csv}"):
                os.makedirs(bronze_directory + f"/{csv}")
                
                
            partition_name = f"{csv}/{csv}.csv"
            filepath = bronze_directory + partition_name
            df.toPandas().to_csv(filepath, index=False)
            print('saved to:', filepath)

    return df

# import os
# import glob
# import pandas as pd
# import matplotlib.pyplot as plt
# import numpy as np
# import random
# from datetime import datetime, timedelta
# from dateutil.relativedelta import relativedelta
# import pprint
# import pyspark
# import pyspark.sql.functions as F
# import argparse

# from pyspark.sql.functions import col
# from pyspark.sql.types import StringType, IntegerType, FloatType, DateType


# def process_bronze_table(snapshot_date_str, bronze_directory, spark, monthly = True):
#     # prepare arguments
#     snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
#     # connect to source back end - IRL connect to back end source system
#     folder_path = "data"
#     # csv_files = [f[:-4] for f in os.listdir(folder_path) if f.endswith('.csv')]

#     if monthly == True:
#         csv_files = ['feature_clickstream','lms_loan_daily']
#         for csv in csv_files:
            
#             # load data - IRL ingest from back end source system
#             # save bronze table to datamart - IRL connect to database to write
#             if csv == 'feature_clickstream':
#                 snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
#                 csv_file_path = f"data/{csv}.csv"
#                 df = spark.read.csv(csv_file_path, header=True, inferSchema=True).filter(F.col('snapshot_date') == snapshot_date)
#                 print(snapshot_date_str + 'row count:', df.count())
#                 if not os.path.exists(bronze_directory + f"/{csv}"):
#                     os.makedirs(bronze_directory + f"/{csv}")
#                 partition_name = f"{csv}/{csv}_" + snapshot_date_str.replace('-','_') + '.csv'
#                 filepath = bronze_directory + partition_name
#                 df.toPandas().to_csv(filepath, index=False)
#                 print('saved to:', filepath)
#             else:
#                 df = spark.read.csv(csv_file_path, header=True, inferSchema=True).filter(F.col('snapshot_date') == snapshot_date)
                
#                 snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
#                 csv_file_path = f"data/{csv}.csv"
#                 csv = "lms_loan_monthly"
#                 df = spark.read.csv(csv_file_path, header=True, inferSchema=True).filter(F.col('snapshot_date') == snapshot_date)
#                 print(snapshot_date_str + 'row count:', df.count())
                
#                 if not os.path.exists(bronze_directory + f"/{csv}"):
#                     os.makedirs(bronze_directory + f"/{csv}")
#                 partition_name = f"{csv}/{csv}_" + snapshot_date_str.replace('-','_') + '.csv'
#                 filepath = bronze_directory + partition_name
#                 df.toPandas().to_csv(filepath, index=False)
#                 print('saved to:', filepath)

#     else:
#         csv_files = ['features_attributes','features_financials']
#         for csv in csv_files:
#             csv_file_path = f"data/{csv}.csv"
#             df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
#             print(snapshot_date_str + 'row count:', df.count())
#             if not os.path.exists(bronze_directory + f"/{csv}"):
#                 os.makedirs(bronze_directory + f"/{csv}")
#             partition_name = f"{csv}/{csv}.csv"
#             filepath = bronze_directory + partition_name
#             df.toPandas().to_csv(filepath, index=False)
#             print('saved to:', filepath)

#     return df
