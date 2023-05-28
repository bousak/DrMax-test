# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Project CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Import Libraries

# COMMAND ----------

import os
import platform
import xmltodict
import pandas as pd
import numpy as np
import json
import requests
from requests.structures import CaseInsensitiveDict
from datetime import datetime
import datetime
from pyspark.sql import DataFrame

import pyspark
from functools import reduce
from pyspark.sql.functions import col
from azure.storage.blob import BlockBlobService
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans, BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler, MinMaxScaler

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### RFM Config

# COMMAND ----------

train_config = {
    'train_date': ['2022-01-01', '2022-02-01', '2022-03-01', '2022-04-01'],
    'eval_dates': ['2022-02-01', '2022-03-01', '2022-04-01',
                   '2022-05-01', '2022-06-01', '2022-07-01',
                   '2022-08-01', '2022-09-01', '2022-10-01',
                   '2022-11-01', '2022-12-01'],
    'overall': ['2022-09-01', '2022-10-01', '2022-11-01', '2022-12-01']
}
TO_CONT = 'customer-segmentation'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Blob Storage Backend

# COMMAND ----------

STORAGE_ACCOUNT = os.getenv('dsBlobStorage')
ACCESS_KEY = os.getenv('dsBlobToken')
CONT_PATH = 'wasbs://{}@{}.blob.core.windows.net'
ACCOUNT_KEY = 'fs.azure.account.key.{}.blob.core.windows.net'.format(STORAGE_ACCOUNT)
BLOCK_BLOB_SERVICE = BlockBlobService(account_name=STORAGE_ACCOUNT, account_key=ACCESS_KEY)

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
             .config('spark.executor.cores', 1)
             .config('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 64)
             .getOrCreate())

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

def list_blob_files(blob_loc, cont_nm, block_blob_service=BLOCK_BLOB_SERVICE):
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

def rem_blob_files(blob_loc, cont_nm, block_blob_service=BLOCK_BLOB_SERVICE):
    # remove files
    names = list_blob_files(blob_loc=blob_loc, cont_nm=cont_nm, block_blob_service=block_blob_service)
    for i in names:
        block_blob_service.delete_blob(cont_nm, i)

    # remove master folder
    master_blob_loc = blob_loc.replace('/' + blob_loc.split('/')[-1], '')
    names = list_blob_files(blob_loc=master_blob_loc, cont_nm=cont_nm, block_blob_service=block_blob_service)
    for i in names:
        if blob_loc == i:
            block_blob_service.delete_blob(cont_nm, i)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### ETL

# COMMAND ----------


class ETL:

    def __init__(
            self,
            src_file: str,
            destination_file: str,
            transformation_code: str,
            src_cont_nm: str,
            destination_cont_nm: str
    ) -> None:
        self.src_file = src_file
        self.destination_file = destination_file
        self.transformation_code = transformation_code
        self.src_cont_nm = src_cont_nm
        self.destination_cont_nm = destination_cont_nm

    def extract(self) -> pyspark.sql.DataFrame:
        """This function extracts parquet file from source and returns spark DataFrame.
        :return:
            DataFrame
        """

        tmp_file_path = '{}/{}'.format(CONT_PATH.format(self.src_cont_nm, STORAGE_ACCOUNT), self.src_file)
        return spark.read.format('parquet').load(tmp_file_path)

    def transform(self) -> pyspark.sql.DataFrame:
        """This function transforms extracted DataFrame and returns spark DataFrame.
        :return:
            DataFrame
        """

        tmp_data = self.extract()
        tmp_data.createOrReplaceTempView('tmp_data')
        return spark.sql(self.transformation_code.format('tmp_data'))

    def load(self) -> None:
        """This function writes transformed DataFrame into destination container.
        :return:
            None
        """

        # read and transform data
        tmp_data = self.transform()

        # remove files from directory
        rem_blob_files(blob_loc=self.destination_file,
                       cont_nm=self.destination_cont_nm,
                       block_blob_service=BLOCK_BLOB_SERVICE)

        # write to AZURE
        tmp_file_path = '{}/{}'.format(CONT_PATH.format(self.destination_cont_nm, STORAGE_ACCOUNT),
                                       self.destination_file)
        tmp_data.write.mode('overwrite').option('header', 'true').format('parquet').save(tmp_file_path)

    def run(self):
        self.load()


# COMMAND ----------

class TransformLoad:

    def __init__(self,
                 destination_file: str,
                 transformation_code: str,
                 destination_cont_nm: str
                 ) -> None:
        self.destination_file = destination_file
        self.transformation_code = transformation_code
        self.destination_cont_nm = destination_cont_nm

    def transform(self) -> DataFrame:
        """This function transforms extracted DataFrame and returns spark DataFrame.
        :return:
            DataFrame
        """

        return spark.sql(self.transformation_code)

    def load(self):
        """This function writes transformed DataFrame into destination container.
        :return:
            DataFrame
        """

        # read and transform data
        tmp_data = self.transform()

        # remove files from directory
        rem_blob_files(blob_loc=self.destination_file,
                       cont_nm=self.destination_cont_nm,
                       block_blob_service=BLOCK_BLOB_SERVICE)

        # write to AZURE
        tmp_file_path = '{}/{}'.format(CONT_PATH.format(self.destination_cont_nm, STORAGE_ACCOUNT),
                                       self.destination_file)
        tmp_data.write.mode('overwrite').option('header', 'true').format('parquet').save(tmp_file_path)

    def run(self):
        self.load()
