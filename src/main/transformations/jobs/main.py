import datetime
import logging
import os.path
import shutil
import sys
from sys import prefix

from Cryptodome.SelfTest.Cipher.test_OFB import file_name
from pyspark.sql.connect.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from rsa.cli import decrypt

from resources.dev import config
from resources.dev.config import bucket_name, local_directory, mandatory_columns, error_folder_path_local, \
    product_staging_table, sales_team_table
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import *
from  src.main.utility.logging_config import *
from  src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter
# from src.test.sales_data_upload_s3 import s3_directory, local_file_path
# from src.test.scratch_pad import folder_path, s3_absolute_file_path

################# S3 client ##############################
aws_access_key = decrypt(config.aws_access_key)

aws_secret_key = decrypt(config.aws_secret_key)

# access_key = decrypt(aws_access_key)
# secret_key = decrypt(aws_secret_key)
s3_client_provider = S3ClientProvider(aws_access_key,aws_secret_key)
s3_client = s3_client_provider.get_client()

####Now you can use s3 client for your operation ####

response = s3_client.list_buckets()
print(response)
logger.info("List of Buckets :%s", response["Buckets"])

#check local directory has already a file.
#if file is there then check then check if the same file is present in staging area
#with status as A. If so then delete try to re run
#else give an error and not process the next file

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_file =[]
if csv_files:
    for file in csv_files:
        total_csv_file.append(file)

    statement = f"select distinct file_name from"\
                f"Vivek_de_project11.product_staging_table"\
                f"where file_name is({str(total_csv_file)[1:-1]}) and status = 'I'"
    logger.info(f"dynamically statement created: {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("your last run was failed please check")

    else:
        logger.info("No record Match")

else:
    logger.info("Last run was successful!!!")


try:
    s3_reader = S3Reader()
    #bucket should now come from table
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,
                                                 config.bucket_name,
                                                 folder_path= folder_path)
    logger.info("Absolute path on s3 bucket for csv file %s",s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at{folder_path}")
        raise Exception("No Data available to process")

except Exception as e:
    logger.info("Exited with error:- %s",e)
    raise e

bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
msg = "file path available on s3 under %s bucket and folder name is %s"
logging.info(msg,'args: bucket_name, file_paths')
logging.info(f"File path available on s3 under {bucket_name} bucket and folder name is {file_paths}")
try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File dowmload error :%s",e)
    sys.exit()

#get list of all file in local directory
all_files = os.listdir(local_directory)
logger.info(f"List of files present at my local directory after download {all_files}")

#Filter files with ".csv" in their names and create absolute paths
if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory,files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory,files)))

    if not csv_files:
        logger.error("No csv data available to process the request")
        raise Exception("No csv data available to process the request")

else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")

####### make csv lines convert into a list of comma seperated ###########

#csv_files = str(csv_files)[1:-1]

logger.info("***********Listing the File**************")
logger.info("List of csv files that need to be processed %s",csv_files)

logger.info("************Creating  Spark Session************")

spark = spark_session()
logger.info("************Spark session Created***************")

#check the required column in schema of csv file
#if not required keep column it in list or error file
#else union all data into data frame

logger.info("********checking schema for data loaded in S3***********")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format(".csv")\
        .option("header","true")\
        .load(data).columns
    logger.info(f"Schema for  the {data} is {data_schema}")
    logger.info(f"Mandatory column schema is {config.mandatory_columns}")
    missing_column = set(config.mandatory_columns)- set(data_schema)
    logger.info(f"missing column are {missing_column}")

    if missing_column:
        error_files.append(data)
    else:
        logger.info(f"No missing column for the {data}")
        correct_files.append(data)

logger.info(f"***********List of correct File**************{correct_files}")
logger.info(f"***********List of error File**************{error_files}")
logger.info(f"*********Moving error data to error directory if any **************")

#move data to error directory on local
error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path,file_name)

            shutil.move(file_path,destination_path)
            logger.info(f"Moved'{file_name}'from S3 file path to '{destination_path}'.")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix,file_name)
            logger.info(f"{message}")
        else:
            logger.error(f"'{file_path}' does not exist.")

else:
    logger.info("***********There is no error File available at our dataset**************")

#Before running the process
#stage table needs to be updated with status as active (A) or inactive (I)
logger.info("***********updating product_staging_table that we have started process**************")
Insert_statement = []
db_name =config.database_name
current_date = datetime.datetime.now()
formatted_date =current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statement = f"Insert into {db_name}.{config.product_staging_table}"\
                    f"(file_name, file_location, created_date, status)"\
                    f"VALUES ('{filename}','{filename}','{formatted_date}','A')"

        Insert_statement.append(statement)
    logger.info(f"Insert statement created for staging table ----{Insert_statement}")
    logger.info("***********Connecting with mysql server**************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("***********MY SQL server connected successfully**************")
    for statement in Insert_statement:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("***********There is no File to process**************")
    raise Exception ("***********No data available with correct File**************")

logger.info("***********Staging Table updated successfully**************")

logger.info("***********Fixing Extra column coming from source **************")

schema = StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("store_id",IntegerType(),True),
    StructField("product_name",StringType(),True),
    StructField("sales_date",DateType(),True),
    StructField("sales_person_id",IntegerType(),True),
    StructField("price",FloatType(),True),
    StructField("quantity",IntegerType(),True),
    StructField("total_cost",FloatType(),True),
    StructField("additional_column",StringType(),True)
])

#connecting with DatabaseReader
database_client = DatabaseReader(config.url,config.properties)
logger.info("***********creating empty dataframe**************")
final_df_to_process = database_client.create_dataframe(spark,"empty_df_create_table")

#final_df_to_process = spark.createDataFrame([], schema = schema)
#create a new column with concatenated value of extra columns
for data in correct_files:
    data_df = spark.read.format("csv")\
        .option("header","true")\
        .option("inferSchema","true")\
        .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra column present at source is {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn("additional_column",concat_ws(", ",*extra_columns))\
            .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity",
                    "total_cost","additional_column")
        logger.info(f"processed{data} and added 'additional column" )
    else:
        data_df = data_df.withColumn("additional_column",lit(None))\
            .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity",
                    "total_cost","additional_column")

    final_df_to_process = final_df_to_process.union(data_df)
# final_df_to_process = data_df
logger.info("***********Final Dataframe from source which will going to processing**************")
final_df_to_process.show()

#enrich the data from all dimension table
#also create a datamart for sales_team and their incentive, address and all
#another datamart for customer who bought how much each days of month
#for every month there should be a file and inside that
#there should be a store_id segrigation
#read data from parquet and generate csv file
#in which there will be a sales_person_name, sales_person_store_id
#sales_person_total_billing_done_for_each_month, total_incentive

#connecting with DatabaseReader
database_client = DatabaseReader(config.url,config.properties)
#creating df for all tables
#customer table
logger.info("***********Loading customer table into customer_table_df**************")
customer_table_df = database_client.create_dataframe(spark,config.customer_table_name)
#product table
logger.info("***********Loading product table into product_table_df**************")
product_table_df = database_client.create_dataframe((spark,config.product_table))

#product_staging_table table
logger.info("***********Loading staging table into product_staging_table_df**************")
product_staging_table_df = database_client.create_dataframe(spark,product_staging_table)

#sales_team table
logger.info("***********Loading sales team table into sales_team_table_df**************")
sales_team_table_df = database_client.create_dataframe(spark,config.sales_team_table)

#store_table
logger.info("***********Loading store table into store_table_df**************")
store_table_df =database_client.create_dataframe(spark,config.store_table)

s3_customer_store_sales_df_join = dimesions_table_join((final_df_to_process,
                                                        customer_table_df,
                                                        store_table_df,
                                                        sales_team_table_df))

#Final enriched data
logger.info("***********Final Enriched Data**************")
s3_customer_store_sales_df_join.show()


#write the data into customer data mart in parquet format
#file will be written to local first
#move ROW data to s3 bucket for reporting tool
#write reporting data into MYSQL table also
logger.info("***********Write the data into Customer Data Mart**************")
final_customer_data_mart_df = s3_customer_store_sales_df_join\
                                .select("ct.customer_id",
                                        "ct.first_name","ct.last_name","ct.address"
                                        ,"ct.pincode","phone_number",
                                        "sales_date","total_cost")
logger.info("***********Final Data for customer Data Mart**************")
final_customer_data_mart_df.show()

parquet_writer = ParquetWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df,config.customer_data_mart_local_file)

logger.info(f"***********customer data writtern to local disk at {config.customer_data_mart_local_file}**************")

#move data on s3 bucket for customer_data_mart
logger.info("***********Data movement from local to s3 for customer data mart**************")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.customer_data_mart_local_file)
logger.info(f"{message}")

#sales_team Data mart
logger.info("***********Write the Data into sales team Data Mart**************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
            .select("store_id",
                    "sales_person_id","sales_person_first_name","sales_person_last_name",
                    "store_manager_name","manager_id","is_manager",
                    "sales_person_address","sales_person_pincode",
                    "sales_date","total_cost",
                    expr("SUBSTRING(sales_date,1,7)as sales_month"))

logger.info("***********Final Data for sale team Data Mart**************")
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,config.sales_team_data_mart_local_file)
logger.info(f"***********sales team data written to local disk at {config.sales_team_data_mart_local_file}**************")
#Move data on s3 bucket for sales_data_mart
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,
                                   config.bucket_name,
                                   config.sales_team_data_mart_local_file)
logger.info(f"{message}")

# also writing data into partitions
final_sales_team_data_mart_df.write.format("parquet")\
            .option("header","true")\
            .mode("overwrite")\
            .partitionBy("sales_month","store_id")\
            .option("path",config.sales_team_data_mart_partitioned_local_file)\
            .save()
#moved data on s3 for partitioned folder
s3_prefix = "sales_partitioned_data_mart"
current_epach = int(datetime.datetime.now().timestamp())*1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root,file)
        relative_file_path = os.path.relpath(local_file_path,config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epach}/{relative_file_path}"
        s3_client.upload_file(local_file_path,config.bucket_name,s3_key)

#calculation for customer mart
#find out the customer total purchase every month
#write data into MYSQL table
logger.info("***********Calculating customer every month purchase amount**************")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("***********Calculation of customer mart done and written into the table**************")

#calculation for sales team mart
#find out the total sales done by each sales person every month
#Give the top performer 1% incentive of total sales of month
#Rest sales person will get nothing
#write the data into MYSQL table