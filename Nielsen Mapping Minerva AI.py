# Databricks notebook source
#To import all functions
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import concat, col, lit,upper, udf, array
from pyspark.sql.types import StringType, BooleanType, ArrayType, IntegerType, DoubleType
from datetime import datetime
from itertools import chain
import requests


#Server info to pull NSR and Nielsen Data
GCK_Server_Address = "????"
GCK_databatename = "???"
GCK_username = "???"
GCK_passwordname = "???" 

GCK_connection_string = "jdbc:sqlserver://" +GCK_Server_Address +"database="+ GCK_databatename + "user="+ GCK_username +"password="+GCK_passwordname+"encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


#Line 18 ~32 remain unchanged at all time unless we need to access another Minerva service
verbos = 'false'
timezoneOffset = '0'

endpoint = '?????'
authoring_key = '????'

app_config = {
  'BEAT - Brand' : 'dfcf10a6-f8ee-4e07-a1c0-54b202211399',
  'BEAT - Flavour' : 'a39ee405-535d-4e44-828f-c207a545930b',
  'BEAT - Package Type' : 'e599bc29-726f-4e0d-a54b-321d7e0089c4',
  'BEAT - Package Size' : 'f93c0993-be00-475a-974a-ae8edacd52de',
  'BEAT - Secondary Container Type' : '7e0c59aa-d01a-44fc-91c6-2b799d4f54a6'
}

app_version = "2.0"

geocode = "??"
process = "???"

#We are limiting it to 100 rows for testing purpose to save budget. Also only keeping what's necessary.
query = """(select top (100) 
              BRAND,
              FLAVOR,
              PACKAGE as PACK_TYPE,
              pack_size as PACK_SIZE,
              Pack_Unit as UNITS_PER_PACKAGE
          FROM [dbo].[Dim_Nielsen_Product]
          where MANUFACTURER IN ('TCCC', 'TCCC ( Int. )', 'TCCC ( Lo. )', 'Monster') )a"""

# we are renaming some of the column 
data = data.withColumnRenamed("BRAND","BEAT - Brand")\
          .withColumnRenamed("FLAVOR","BEAT - Flavour")\
         .withColumnRenamed("PACKAGE",'BEAT - Package Type')\
         .withColumnRenamed("pack_size",'BEAT - Package Size')\
         .withColumnRenamed("Pack_Unit",'BEAT - Secondary Container Type')

#need to write a query here to only take the what's needed.
db_name = "GCK_DW"
table_name = """(SELECT distinct
                 prod.bpp_cd as BPP
                ,prod.[brnd_nm] as BRAND
                ,prod.[bu_prod_desc]
                ,pkg.[bpp_nm]
                ,prod.[flvr_nm] as FLAVOR
                ,prod.[flvr_cat_nm]
                ,prod.[calr_nm]
                ,prod.[lcl_hier_3_l2_desc]
                ,pkg.[bpp_cd]    
                ,pkg.[pkg_cd]
                ,pkg.[pkg_nm]
                ,pkg.[unt_p_pack_qty]
                ,pkg.[unt_p_case_qty] as UNITS_PER_PACKAGE
                ,pkg.[pkg_p_case_qty]
                ,pkg.[lcl_hier_1_l2_desc] as PACK_SIZE
                ,pkg.[lcl_hier_2_l3_desc]
                ,pkg.[lcl_hier_3_l2_desc] as PACK_TYPE
            FROM [dbo].[Dim_NSR_Prod] prod
            LEFT JOIN [Dim_NSR_Pkg] pkg
            ON pkg.[prod_id] = prod.[prod_id]) a """

#Function to read table
def read_table (tablename):
  df=spark.read\
    .format("jdbc")\
    .option("url",GCK_connection_string)\
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .option("dbtable",tablename)\
    .load()
  return df

# The function is getting the unique values from the params dictionary (/mnt/beat/config/minerva_config.csv), and enriching the data set with   col_intent and col_score for each of the params.
def query_luis(utterance, app_name):
  app_id = app_config[app_name]
  try:
    r = requests.get('https://{}/luis/v{}/apps/{}'.format(endpoint,app_version, app_id), \
                            params = {'verbose': verbos,\
                                      'timezoneOffset': timezoneOffset,\
                                      'subscription-key': authoring_key,\
                                      'q': utterance}).json()['topScoringIntent']
    return (r['intent'] , r['score'])
  except:
    return (None, None)


# The function is getting the unique values from the params dictionary (/mnt/beat/config/minerva_config.csv), and enriching the data set with col_intent and col_score for each of the params.
def luis_handler(params, df):
  for app_name, column_name in params:
    luis_col_name = 'LUIS_'+column_name
    intent_col_name = column_name + "_INTENT"
    score_col_name = column_name + "_SCORE"
    luis_query_schema = StructType([StructField(intent_col_name, StringType(), False),
                                    StructField(score_col_name, DoubleType(), False)])

    query_luis_udf = udf(query_luis,luis_query_schema)
    luis_map = df.select(column_name).where('{} is not null'.format(column_name))\
                                          .distinct()\
                                          .withColumn(luis_col_name, query_luis_udf(col(column_name), lit(app_name)))\
                                          .select(column_name, luis_col_name+'.*')
    
    df = df.join(luis_map, df[column_name] == luis_map[column_name], how = 'left')\
                     .select(df['*'], luis_map[intent_col_name], luis_map[score_col_name])
  return df

# Get the BPP code for each of the intents
def intents_to_bpp(*data):
  data_map = list(zip(luis_cols, data))
  df = bpp_df.copy()  
  
  for col_name, col_value in data_map:
    if col_value not in ('None', 'null'):
      series_ = df[col_name] == col_value
      if series_.any():
        df = df[series_]
  return df.iloc[0]['BPP']


# COMMAND ----------

######################## COMMAND ########################
bpp = read_table(table_name)  #NSR Data
bpp = bpp.withColumn('BRAND', upper(col('BRAND')))\
  .withColumn('FLAVOR', upper(col('FLAVOR')))\
  .withColumn('PACK_TYPE', upper(col('PACK_TYPE')))\
  .withColumn('PACK_SIZE', upper(col('PACK_SIZE')))\
  .withColumn('UNITS_PER_PACKAGE', upper(col('UNITS_PER_PACKAGE'))).distinct()
bpp_df = bpp.toPandas()

#To read in our config file. We only need to update the config file. No need to change the codes here.Currently, the file is in Chinadatalake blob storage.
minerva_config_path = "/mnt/Data Maps/minerva_config_gck.csv"
minerva_config = spark.read.format('csv').options(header='true', inferSchema='true').option("delimiter", ',').load(minerva_config_path)

minerva_config_df = minerva_config.toPandas()

luis_params = minerva_config_df[(minerva_config_df['Geo'] == geocode) & 
                                (minerva_config_df['Process'] == process)][['BEAT - Brand',
                                                                              'BEAT - Flavour',
                                                                              'BEAT - Package Type',
                                                                              'BEAT - Package Size',
                                                                              'BEAT - Secondary Container Type']].to_dict('records')[0]

luis_params = [(key_, luis_params[key_]) for key_ in luis_params.keys() if luis_params[key_] is not None]
# [('BEAT - Brand', 'BRAND'), ('BEAT - Flavour', 'FLAVOR'), ('BEAT - Package Type', 'PACK_TYPE'), ('BEAT - Package Size', 'PACK_SIZE'), ('BEAT - Secondary Container Type', 'UNITS_PER_PACKAGE')]

luis_cols = [item[1] for item in luis_params]
#['BRAND', 'FLAVOR', 'PACK_TYPE', 'PACK_SIZE', 'UNITS_PER_PACKAGE']

luis_intent_cols = [col+'_INTENT' for col in luis_cols]
#['BRAND_INTENT', 'FLAVOR_INTENT', 'PACK_TYPE_INTENT', 'PACK_SIZE_INTENT', 'UNITS_PER_PACKAGE_INTENT']

luis_intent_cols_na = {item : 'No Data' for item in luis_intent_cols}
#{'BRAND_INTENT': 'No Data', 'FLAVOR_INTENT': 'No Data', 'PACK_TYPE_INTENT': 'No Data', 'PACK_SIZE_INTENT': 'No Data', 'UNITS_PER_PACKAGE_INTENT': 'No Data'}

# COMMAND ----------

# To register the function  
intents_to_bpp_udf = udf(intents_to_bpp)

#The actual code to query the Minerva AI service and joining the NSR with the Nielsen (After Minerva AI service) to create a map. However we need to feed more data to the AI service because some of the _intent are either None (Null) or apparently not right.

data = read_table(query)
data_luis = luis_handler(luis_params, data)
data_luis = data_luis.fillna(luis_intent_cols_na)
luis_distinct = data_luis.select(luis_intent_cols).distinct().withColumn('BPP', intents_to_bpp_udf(*luis_intent_cols))
data_luis_bpp = data_luis.join(luis_distinct, on = luis_intent_cols,how = 'left').select(data_luis['*'],luis_distinct.BPP)
display(data_luis_bpp)


################################################################################################################
#To create a mapping table and join with the whatever product table that's in the SQL server if there are same product (SKU) existed in the #system, we are only taking the one with the largest volumn (original logic). However we could do it our way here i.e to do all the logic here # then send the mapping table back to SQL server. 

#data_luis_bpp.createTempView('master_mapped')
# spark.sql(f'drop table if exists {db_name}.{table_name}')

# spark.sql(f"""
# create table {db_name}.{table_name}

# select a.*, b.PROD_ID from master_mapped a
# left join (select BPP, PROD_ID from (
#           select BPP, PROD_ID, ROW_NUMBER() OVER (PARTITION BY BPP ORDER BY VOLUME DESC) rank from {db_name}.ISSCOM_BPP_PRODUCT_ID)
#           where rank = 1) b
# on a.BPP = b.BPP
# """)
###############################################################################################################

# COMMAND ----------


