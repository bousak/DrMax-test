# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Setup

# COMMAND ----------

# MAGIC %run /Repos/adm.test.bouse@drmaxtest.onmicrosoft.com/DrMax-test/CONFIG_RFM

# COMMAND ----------

import platform

if platform.uname().node == 'GL22WD4W2DY33':
    from setup.CONFIG import *

BLOB_NAME = 'customer-segmentation'
STORED_FOLDER = 'ecomm_rfm_model/master_data'
W_PATH = '{}/{}'.format(CONT_PATH.format(BLOB_NAME, STORAGE_ACCOUNT), STORED_FOLDER)
rem_blob_files(blob_loc=STORED_FOLDER,
               cont_nm=BLOB_NAME,
               block_blob_service=BLOCK_BLOB_SERVICE)

# COMMAND ----------

print(platform.collections)

# COMMAND ----------

spark.sql("select current_date() as today;").show()

# COMMAND ----------

spark.sql("select count(*) from parquet.`wasbs://customer-segmentation@2021azureml.blob.core.windows.net/ecomm_rfm_model/master_data/`;").show()

# COMMAND ----------

general_overview_code = '''
select 
  count(*),
from parquet.`wasbs://customer-segmentation@2021azureml.blob.core.windows.net/ecomm_rfm_model/master_data/`
'''

general_overview_db = spark.sql(general_overview_code)
general_overview_db.createOrReplaceTempView('general_overview_db')
# general_overview_db.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Data Loading

# COMMAND ----------

general_overview_code = '''
select 
  *,
  count(*) over(partition by country, customer_email order by created_at asc rows between unbounded preceding and 1 preceding) as n_distinct_previous_orders  
from
(
 select 
     created_at,
     'IT' as country,
     global_currency_code,
     increment_id,
     customer_email,
     customer_id,
     customer_is_guest,
     case when customer_is_guest is null then 'unregistered' when customer_is_guest = 1 then 'unregistered' else 'registered' end as is_registered_order,
     case 
      when discount_description like '%Default Stock%' then null
      when discount_description like '%shipping_assignments%' then null
       else discount_description end as discount_description,
     state,
     status,
     case 
      when coupon_code like '%Default Stock%' then null
      when coupon_code like '%shipping_assignments%' then null
       else coupon_code end as coupon_code,
     payment_method,
     shipping_method,
     shipping_description,
     drmax_order_type,
     
     avg(discount_amount) as discount_amount,
     avg(shipping_amount) as shipping_amount,
     avg(total_paid) as total_paid,
     avg(total_invoiced) as total_invoiced,
     avg(total_canceled) as total_canceled,
     avg(total_due) as total_due
 from parquet.`wasbs://magento-backend@2021azureml.blob.core.windows.net/1_data_processing/it_magento_orders`
 group by 
     created_at,
     global_currency_code,
     increment_id,
     customer_email,
     customer_id,
     customer_is_guest,
     discount_description,
     state,
     status,
     coupon_code,
     payment_method,
     shipping_method,
     shipping_description,
     drmax_order_type
     
 union all
 select 
     created_at,
     'SK' as country,
     global_currency_code,
     increment_id,
     customer_email,
     customer_id,
     customer_is_guest,
     case when customer_is_guest is null then 'unregistered' when customer_is_guest = 1 then 'unregistered' else 'registered' end as is_registered_order,
     case 
      when discount_description like '%Default Stock%' then null
      when discount_description like '%shipping_assignments%' then null
       else discount_description end as discount_description,
     state,
     status,
     case 
      when coupon_code like '%Default Stock%' then null
      when coupon_code like '%shipping_assignments%' then null
       else coupon_code end as coupon_code,
     payment_method,
     shipping_method,
     shipping_description,
     drmax_order_type,
     
     avg(discount_amount) as discount_amount,
     avg(shipping_amount) as shipping_amount,
     avg(total_paid) as total_paid,
     avg(total_invoiced) as total_invoiced,
     avg(total_canceled) as total_canceled,
     avg(total_due) as total_due
 from parquet.`wasbs://magento-backend@2021azureml.blob.core.windows.net/1_data_processing/sk_magento_orders`
 group by 
     created_at,
     global_currency_code,
     increment_id,
     customer_email,
     customer_id,
     customer_is_guest,
     discount_description,
     state,
     status,
     coupon_code,
     payment_method,
     shipping_method,
     shipping_description,
     drmax_order_type
 
 union all
 select 
     created_at,
     'PL' as country,
     global_currency_code,
     increment_id,
     customer_email,
     customer_id,
     customer_is_guest,
     case when customer_is_guest is null then 'unregistered' when customer_is_guest = 1 then 'unregistered' else 'registered' end as is_registered_order,
     case 
      when discount_description like '%Default Stock%' then null
      when discount_description like '%shipping_assignments%' then null
       else discount_description end as discount_description,
     state,
     status,
     case 
      when coupon_code like '%Default Stock%' then null
      when coupon_code like '%shipping_assignments%' then null
       else coupon_code end as coupon_code,
     payment_method,
     shipping_method,
     shipping_description,
     drmax_order_type,
     
     avg(discount_amount) as discount_amount,
     avg(shipping_amount) as shipping_amount,
     avg(total_paid) as total_paid,
     avg(total_invoiced) as total_invoiced,
     avg(total_canceled) as total_canceled,
     avg(total_due) as total_due
 from parquet.`wasbs://magento-backend@2021azureml.blob.core.windows.net/1_data_processing/pl_magento_orders`
 group by 
     created_at,
     global_currency_code,
     increment_id,
     customer_email,
     customer_id,
     customer_is_guest,
     discount_description,
     state,
     status,
     coupon_code,
     payment_method,
     shipping_method,
     shipping_description,
     drmax_order_type
     
 union all
 select 
     created_at,
     'RO' as country,
     global_currency_code,
     increment_id,
     customer_email,
     customer_id,
     customer_is_guest,
     case when customer_is_guest is null then 'unregistered' when customer_is_guest = 1 then 'unregistered' else 'registered' end as is_registered_order,
     case 
      when discount_description like '%Default Stock%' then null
      when discount_description like '%shipping_assignments%' then null
       else discount_description end as discount_description,
     state,
     status,
     case 
      when coupon_code like '%Default Stock%' then null
      when coupon_code like '%shipping_assignments%' then null
       else coupon_code end as coupon_code,
     payment_method,
     shipping_method,
     shipping_description,
     drmax_order_type,
     
     avg(discount_amount) as discount_amount,
     avg(shipping_amount) as shipping_amount,
     avg(total_paid) as total_paid,
     avg(total_invoiced) as total_invoiced,
     avg(total_canceled) as total_canceled,
     avg(total_due) as total_due
 from parquet.`wasbs://magento-backend@2021azureml.blob.core.windows.net/1_data_processing/ro_magento_orders`
 group by 
     created_at,
     global_currency_code,
     increment_id,
     customer_email,
     customer_id,
     customer_is_guest,
     discount_description,
     state,
     status,
     coupon_code,
     payment_method,
     shipping_method,
     shipping_description,
     drmax_order_type
 
 union all
 select 
     created_at,
     'CZ' as country,
     global_currency_code,
     increment_id,
     customer_email,
     customer_id,
     customer_is_guest,
     case when customer_is_guest is null then 'unregistered' when customer_is_guest = 1 then 'unregistered' else 'registered' end as is_registered_order,
     case 
      when discount_description like '%Default Stock%' then null
      when discount_description like '%shipping_assignments%' then null
       else discount_description end as discount_description,
     state,
     status,
     case 
      when coupon_code like '%Default Stock%' then null
      when coupon_code like '%shipping_assignments%' then null
       else coupon_code end as coupon_code,
     payment_method,
     shipping_method,
     shipping_description,
     drmax_order_type,
     
     avg(discount_amount) as discount_amount,
     avg(shipping_amount) as shipping_amount,
     avg(total_paid) as total_paid,
     avg(total_invoiced) as total_invoiced,
     avg(total_canceled) as total_canceled,
     avg(total_due) as total_due
 from parquet.`wasbs://magento-backend@2021azureml.blob.core.windows.net/1_data_processing/cz_magento_orders`
 group by 
     created_at,
     global_currency_code,
     increment_id,
     customer_email,
     customer_id,
     customer_is_guest,
     discount_description,
     state,
     status,
     coupon_code,
     payment_method,
     shipping_method,
     shipping_description,
     drmax_order_type
)
'''

general_overview_db = spark.sql(general_overview_code)
general_overview_db.createOrReplaceTempView('general_overview_db')
# general_overview_db.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Writing to AZURE

# COMMAND ----------

(spark.sql('select * from general_overview_db')
 .write
 .mode('overwrite')
 .option('header', 'true')
 .format('parquet')
 .save(W_PATH))
