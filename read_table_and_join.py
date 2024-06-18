import sys
import pytz
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
import datetime 
from datetime import datetime,timedelta
import pandas as pd
import boto3


args = getResolvedOptions(sys.argv, ["JOB_NAME", "WORKFLOW_NAME", "WORKFLOW_RUN_ID"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
    
glue_client = boto3.client("glue")
workflow_properties = glue_client.get_workflow_run_properties(Name=args["WORKFLOW_NAME"], RunId=args["WORKFLOW_RUN_ID"])["RunProperties"]

business_date_s = workflow_properties['business_date']

table_data_from_db_ddf= glueContext.create_dynamic_frame.from_catalog(database=db_name,table_name=f"db_name}_public_x_table",transformation_ctx="xyz",additional_options = { "hashpartitions": "10" , "sampleQuery":"select id,course from student where roll_number=15" })

def transform_data(row):
    row["vm_100"] = 0.0
   
    return row

table_data_transformed_ddf = table_data_from_db_ddf.map(f = transform_data)

assert (table_data_from_db_ddf.count() == table_data_transformed_ddf.count()), "The count should match after the transformation "



table_data_from_db_df = table_data_from_db_ddf.toDF()
table_data_transformed_df = table_data_transformed_ddf.toDF()

joined_df = table_data_from_db_df.join(table_data_transformed_df, ["id","course"],how='left_outer')
joined_ddf = DynamicFrame.fromDF(joined_df, glueContext,"joined_df")
