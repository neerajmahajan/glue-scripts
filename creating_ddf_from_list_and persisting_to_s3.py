import sys
import pytz
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
import datetime 
from datetime import datetime
import pandas as pd
import boto3

args = getResolvedOptions(sys.argv, ["JOB_NAME", "WORKFLOW_NAME", "WORKFLOW_RUN_ID"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


record_list = list()

record_list.append('record line 1')
record_list.append('record line 2')
record_list.append('record line 3')

pdf = pd.DataFrame(record_list,columns=['records'])
pyspark_df = spark.createDataFrame(pdf)

'''
def build_detail_record(ddf,header_record,trailer_record):
    qp_list = list()
    qp_list.append(header_record)
    for col in ddf.toDF().collect():
        record = f'{col["detail_record_type"],col["rl_number"]}'
        #print(record)
        qp_list.append(record)
    qp_list.append(trailer_record)
    return qp_list
'''
record_ddf = DynamicFrame.fromDF(pyspark_df, glueContext,"s3_data_as_is") 
record_ddf.show()
# create a single file
record_ddf = s3_data_as_is.repartition(1)

glueContext.write_dynamic_frame.from_options(frame = record_ddf,connection_type = "s3",connection_options = {"path": s3_location},format = "csv",
    format_options={"quoteChar": -1,"writeHeader":"false"},)
job.commit()
