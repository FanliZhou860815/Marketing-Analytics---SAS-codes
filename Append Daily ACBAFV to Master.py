from pyutils.versace import *
from datetime import datetime,date,timedelta

inputdate = (date.today() ).strftime('%Y%m%d')
print(inputdate)

filename ="s3://marketing-analytics-pal/sas_transfer/xdb/Saks_XDB_ACBAFV/Saks_XDB_ACBAFV_"+inputdate+".csv"
print(filename)

# read csv  file -- source from xdb noon process
df_acbafv = spark.read.csv(filename, header=True, inferSchema=True)
df_acbafv.createOrReplaceTempView("df_acbafv")
df_acbafv.show(10)

# read xdb acbafv master file
df_acbafv_master = spark.read.parquet(path_s3(schema='source',table="saks_xdb_acbafv/current/"))
df_acbafv_master.createOrReplaceTempView("df_acbafv_master")
df_acbafv_master.show(10)

# qa
spark.sql(f"""
select program_name , max(send_date ) from df_acbafv_master group by 1
""").show(10)

# convert field names from upper case to lower case
df_acbafv_lower = spark.sql(f"""
select
EMAIL_ADDRESS 	as 	email_address ,
      EMAIL_ID 	as 	      email_id ,
      BLUEMARTINIID 	as 	      bluemartiniid ,
      to_date(SEND_DATE) 	as 	      send_date ,
      PROGRAM_NAME 	as 	      program_name ,
      PROGRAM_CODE 	as 	      program_code ,
      PRIORITY 	as 	      priority ,
      TESTING 	as 	      testing ,
      RECIPE 	as 	      recipe ,
      CAT_NAME 	as 	      cat_name,
      REC_STRATEGY 	as 	      rec_strategy,
      PROMO_CODE 	as 	      promo_code ,
      BARCODE 	as 	      barcode ,
      WL_RCVD_STATUS 	as 	      wl_rcvd_status ,
      LI_TYPE  	as 	      li_type ,
      SKU_1  	as 	      sku_1  ,
      SKU_2  	as 	      sku_2  ,
      SKU_3  	as 	      sku_3  ,
      SKU_4  	as 	      sku_4  ,
      SKU_5  	as 	      sku_5  ,
      SKU_6  	as 	      sku_6  ,
      SKU_7  	as 	      sku_7  ,
      SKU_8  	as 	      sku_8  ,
      SKU_9  	as 	      sku_9  ,
      SKU_10  	as 	      sku_10  ,
      SKU_11  	as 	      sku_11  ,
      SKU_12  	as 	      sku_12  ,
      SKU_13  	as 	      sku_13  ,
      SKU_14  	as 	      sku_14  ,
      SKU_15	as 	      sku_15

from df_acbafv

""")
df_acbafv_lower.createOrReplaceTempView("df_acbafv_lower")
df_acbafv_lower.show(10)

# append new data to master table
df_acbafv_master_new = df_acbafv_master.union(df_acbafv_lower)
df_acbafv_master_new.show()
df_acbafv_master_new.createOrReplaceTempView("df_acbafv_master_new")

# qa -make sure new data is appended
spark.sql(f"""
select program_name , max(send_date ) from df_acbafv_master_new group by 1
""").show(10)

# export file to s3 with new data added -dev
df_acbafv_master_new.write.parquet(path_s3(dryrun=var_dryrun, schema='source', table="saks_xdb_acbafv/current/"),mode='overwrite')

# sync from dev to prod
aws s3 sync  s3://marketing-analytics-pal/dev/etl/source/saks_xdb_acbafv/current/  s3://marketing-analytics-pal/prod/etl/source/saks_xdb_acbafv/current/

