# Databricks notebook source
# MAGIC %md
# MAGIC ### Project CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import Libraries

# COMMAND ----------

import os
import platform
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyspark
from azure.storage.blob import BlockBlobService
from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Blob Storage Backend

# COMMAND ----------

STORAGE_ACCOUNT = os.getenv('dsBlobStorage')
ACCESS_KEY = os.getenv('dsBlobToken')
CONT_PATH = 'wasbs://{}@{}.blob.core.windows.net'


# COMMAND ----------

PROCESSOR_NAME = os.getenv('processorName')

if PROCESSOR_NAME == 'LocalPC' or PROCESSOR_NAME == 'Databricks':
    ACCOUNT_KEY = 'fs.azure.account.key.{}.blob.core.windows.net'.format(STORAGE_ACCOUNT)
    BLOCK_BLOB_SERVICE = BlockBlobService(account_name=STORAGE_ACCOUNT, account_key=ACCESS_KEY)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Setting Paths

# COMMAND ----------


loc_ds_path = Path('C:\Users\martin.bouse\OneDrive - Dr. Max BDC, s.r.o\Plocha\ds_projects')


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Spark Session

# COMMAND ----------

if platform.uname().node == 'GL22WD4W2DY33':
    spark = (pyspark
             .sql
             .SparkSession
             .builder
             .appName('General')
             .config('spark.driver.maxResultSize', '16g')
             .config('spark.executor.memory', '16g')
             .config('spark.driver.memory', '16g')
             .getOrCreate())

    spark.conf.set(ACCOUNT_KEY, ACCESS_KEY)
    spark._jsc.hadoopConfiguration().set(ACCOUNT_KEY, ACCESS_KEY)

if PROCESSOR_NAME == 'Databricks':
    spark.conf.set(ACCOUNT_KEY, ACCESS_KEY)
    spark._jsc.hadoopConfiguration().set(ACCOUNT_KEY, ACCESS_KEY)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### General Functions

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### List Filenames in Blob

# COMMAND ----------

def list_blob_files(blob_loc, cont_nm, block_blob_service) -> List:
    names = []
    next_marker = None
    while True:
        blob_list = block_blob_service.list_blobs(cont_nm, prefix=blob_loc + '/', num_results=5000, marker=next_marker)
        for blob in blob_list:
            names.append(blob.name)

        next_marker = blob_list.next_marker
        if not next_marker:
            break

    return names



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Remove files from Blob

# COMMAND ----------


def rem_blob_files(blob_loc, cont_nm, block_blob_service) -> str:
    # remove files
    names = list_blob_files(blob_loc=blob_loc, cont_nm=cont_nm, block_blob_service=block_blob_service)
    for i in names:
        block_blob_service.delete_blob(cont_nm, i, delete_snapshots='include')

    # remove master folder
    master_blob_loc = blob_loc.replace('/' + blob_loc.split('/')[-1], '')
    names = list_blob_files(blob_loc=master_blob_loc, cont_nm=cont_nm, block_blob_service=block_blob_service)
    for i in names:
        if blob_loc == i:
            block_blob_service.delete_blob(cont_nm, i, delete_snapshots='include')

    return 'Blobs Deleted!'



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Blob refreshed in last XYZ hours?

# COMMAND ----------

def blob_validity(blob_file_path, cont_nm, hours_since_last_modified):
    blob_exists = BLOCK_BLOB_SERVICE.exists(container_name=cont_nm,
                                            blob_name=blob_file_path)

    if blob_exists:
        blob_prop = BLOCK_BLOB_SERVICE.get_blob_properties(container_name=cont_nm,
                                                           blob_name=blob_file_path)

        now_date = datetime.now().replace(tzinfo=None)
        modified_date = datetime.astimezone(blob_prop.properties.last_modified).replace(tzinfo=None)
        hours_last_modified = (now_date - modified_date).total_seconds() / (60 * 60)

        if hours_last_modified > hours_since_last_modified:
            old_blob = True
        else:
            old_blob = False

    else:
        old_blob = True
    return old_blob



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create Calendar

# COMMAND ----------

def create_calendar(country, start_date, col_nm, freq, ascending=False) -> DataFrame:
    tmp_db = (pd.DataFrame(data={'created_dt': pd.date_range(start=start_date,
                                                             end=datetime.now(),
                                                             freq=freq)})
              .sort_values(by=col_nm, ascending=ascending)
              .reset_index(drop=True))
    tmp_db['country'] = country

    return tmp_db



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Project Based Functionality

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Get Already Stored Files

# COMMAND ----------

def get_stored_filenm(blob_file_path, cont_nm, storage) -> List:
    get_files_code = """
        select 
            filenm
            , count(*) as n_rows 
        from {}
        group by filenm
    """
    wasbs_path = ('parquet.`wasbs://' + cont_nm + '@' +
                  storage + '.blob.core.windows.net/' + blob_file_path + '`')
    files_db = spark.sql(get_files_code.format(wasbs_path)).toPandas()
    files_nm = list(files_db['filenm'])

    return files_nm



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Get Raw Blobs

# COMMAND ----------

def get_raw_blobs(raw_files_nm, cont_nm, block_blob_service) -> List:
    names_out = list()
    count = 0
    for nm in raw_files_nm:
        tmp_nm = list_blob_files(blob_loc=nm, cont_nm=cont_nm, block_blob_service=block_blob_service)

        if count == 0:
            names_out = tmp_nm
        else:
            names_out = names_out + tmp_nm
        count += 1

    return names_out



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Read Order File

# COMMAND ----------

def read_order_file(file_loc, cont_nm, storage, cont_path) -> pyspark.sql.DataFrame:
    """
    :param file_loc: location of file to be loaded; str
    :param cont_nm:
    :param storage:
    :param cont_path:
    :return: pyspark DataFrame
    """
    file_path = '{}/{}'.format(cont_path.format(cont_nm, storage), file_loc)
    tmp_data = (spark.read
                .format('csv')
                .option('multiLine', 'true')
                .option('allowSingleQuotes', 'false')
                .load(file_path, header=True, inferSchema=False, sep='~', quote='"')
                .withColumn('filenm', input_file_name())
                .withColumn('billa', regexp_replace(col('billing_address'), "'", '"'))
                .withColumn('exta', regexp_replace(col('extension_attributes'), "'", '"'))
                .withColumn('itemsAlt', regexp_replace(regexp_replace(regexp_replace(col('items'), '"', ''), "'", '"'),
                                                       'Gift from rule:', 'Gift from rule'))
                .withColumn('paymentAlt', regexp_replace(regexp_replace(col('payment'), '"', ''), "'", '"')))
    return tmp_data



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Replace missing columns

# COMMAND ----------

def force_columns(data, col_list) -> pyspark.sql.DataFrame:
    """
    :param data: pyspark DataFrame
    :param col_list: List, required columns
    :return: pyspark DataFrame
    """

    for i in col_list:
        if i not in data.columns:
            data = data.withColumn(i, lit(''))
    return data



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Union all files by Name

# COMMAND ----------

def union_all_files(i_file_nm, cols_list, cont_nm, storage, cont_path) -> pyspark.sql.DataFrame:
    """
    :param i_file_nm: Location of files to be loaded and processed; List
    :param cols_list: List of columns required in order file; List
    :param cont_path:
    :param storage:
    :param cont_nm:
    :return: pyspark.sql.DataFrame
    """

    count = 0
    for i in i_file_nm:

        # i = file_nm[0]

        # read .csv
        tmp_read_data = read_order_file(file_loc=i, cont_nm=cont_nm, storage=storage, cont_path=cont_path)

        # force columns
        tmp_force_data = force_columns(tmp_read_data, cols_list)

        # union by name
        if count == 0:
            tmp_out = tmp_force_data
        else:
            tmp_out = tmp_out.unionByName(tmp_force_data, allowMissingColumns=True)
        count += 1

    return tmp_out



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Check columns -> stored vs. processed

# COMMAND ----------

def checkList(firstlist, secondlist):
    # sorting the lists
    firstlist.sort()
    secondlist.sort()

    # if both the lists are equal the print yes
    if firstlist == secondlist:
        res = True
    else:
        res = False

    return res
