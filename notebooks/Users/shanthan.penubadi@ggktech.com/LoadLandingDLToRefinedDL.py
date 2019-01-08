# Databricks notebook source
user_name="DataLakeUser"
password="fun2work@OCC"
database="OralCareCloudDataLake"
server="occpreproduction.database.windows.net"
table_name="etl.SubJobControl"
sub_job_control_id=getArgument("SubJobControlID")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col,lit
from datetime import datetime, timedelta
import jaydebeapi
from pyspark.sql.functions import col,row_number
from pyspark.sql.window import Window
connection = jaydebeapi.connect("com.microsoft.sqlserver.jdbc.SQLServerDriver",
                         "jdbc:sqlserver://"+server+":1433;database="+database,
                           [user_name,password],
                           )
curs = connection.cursor()

# COMMAND ----------

refined_status_query="UPDATE {0} SET RefinedStatus='In Progress' where SubJobControlID={1}".format(table_name,sub_job_control_id)
curs.execute(refined_status_query)

# COMMAND ----------

def LoadLandingDLToRefinedDL():
  spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
  spark.conf.set("dfs.adls.oauth2.client.id", "f2b0fc8c-7b86-4723-9f9c-2738590ec550")
  spark.conf.set("dfs.adls.oauth2.credential", "qrk/QXmSaidsMuR3lapqXyA1s7QtsmlulPNxRMByodg=")
  spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/mpmcmichaelmmm.onmicrosoft.com/oauth2/token")
    
  landing_server= getArgument("LandingServer")
  landing_path = getArgument("LandingPath")
  landing_file = getArgument("LandingObject")
  landing_file_path = 'adl://'+landing_server+'.azuredatalakestore.net/' + landing_path + '/'+ landing_file
  
  refined_server= getArgument("RefinedServer")
  refined_path = getArgument("RefinedPath")
  refined_file = getArgument("RefinedObject")
  refined_file_path = 'adl://'+refined_server+'.azuredatalakestore.net/' + refined_path + '/'+ refined_file
  staging_file_path='adl://'+refined_server+'.azuredatalakestore.net/'+ refined_path + '/'+ refined_file+'_stage'
  archive_folder_path='adl://'+refined_server+'.azuredatalakestore.net/'+ refined_path + '/Archive/'+refined_file
  date=datetime.now().strftime("%Y%m%d%H%M%S")
  old_file_path=archive_folder_path+"/"+refined_file+"_"+date
  ProcessedDateTime=datetime.now()
  
  is_incremental=getArgument("IsIncremental")
  if(is_incremental=='0'):
    final_df_timestamp_off= spark.read.parquet(landing_file_path)
    final_df=final_df_timestamp_off.select(col("*"),lit(ProcessedDateTime).alias("ProcessedDateTime"))
  else:
    key_columns=getArgument("KeyColumns")
    key_columns_list=key_columns.split(",")
    try:
      base_df= spark.read.parquet(refined_file_path)
      delta_df= spark.read.parquet(landing_file_path)
      base_df_columnnames= [columns for columns in base_df.columns]
      delta_df_timestamp=delta_df.select(col("*"),lit(ProcessedDateTime).alias("ProcessedDateTime"))
      update_df=base_df.select(key_columns_list).intersect(delta_df_timestamp.select(key_columns_list))
      base_df_after_del=(base_df.join(update_df, key_columns_list, "leftanti")).select(base_df_columnnames)
      final_df=base_df_after_del.union(delta_df_timestamp).distinct()
    except:
      landing_file_path = 'adl://'+landing_server+'.azuredatalakestore.net/' + landing_path +'/*'
      landing_df=spark.read.parquet(landing_file_path)
      landing_df_columns=landing_df.columns
      for column in landing_df_columns:
        if "Changeutc".upper()==column.upper():
          order_column=column
          break
        elif "Createutc".upper()==column.upper():
          order_column=column
      W=Window\
        .partitionBy(key_columns_list)\
        .orderBy(col(order_column).desc())
      final_df_timestamp_off=(landing_df.select(row_number().over(W).alias("row_num"), col("*")).where(col("row_num")==1)).select(landing_df_columns).distinct()      
      final_df=final_df_timestamp_off.select(col("*"),lit(ProcessedDateTime).alias("ProcessedDateTime"))
  dbutils.fs.rm(staging_file_path,True)
  final_df.write.parquet(staging_file_path)
  dbutils.fs.mkdirs(archive_folder_path)
  try:
    dbutils.fs.mv(refined_file_path ,old_file_path,True)
  except:
    pass
  dbutils.fs.mv(staging_file_path ,refined_file_path,True)
  date_archive=['9999999999999999']
  for i in dbutils.fs.ls(archive_folder_path):
    date_archive.append((i.path)[-15:len(i.path)-1])
  del_file_date=min(date_archive)
  del_file=archive_folder_path+"/"+refined_file+"_"+del_file_date 
  if(len(dbutils.fs.ls(archive_folder_path))>3):
     dbutils.fs.rm(del_file,True)

# COMMAND ----------

try:
  LoadLandingDLToRefinedDL()
except Exception as e:
  error_message=str(e)
  error_message=error_message.strip('\'')
  if(len(error_message)>2000):
    error_message=error_message[:2000]
  refined_logging_query="UPDATE {0} SET RefinedStatus='Failed',RefinedErrorMessage='{1}',RefinedEndTime=SYSUTCDATETIME() ,SubJobStatus='Failed' WHERE SubJobControlID={2}".format(table_name,error_message,sub_job_control_id)
  curs.execute(refined_logging_query)
  raise Exception
refined_logging_query="UPDATE {0} SET RefinedStatus='Completed',RefinedEndTime=SYSUTCDATETIME() ,RefinedErrorMessage=NULL ,SubJobStatus='Completed'  WHERE SubJobControlID={1}".format(table_name,sub_job_control_id)
curs.execute(refined_logging_query)