import os
import sys
import hashlib
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from datetime import datetime
import boto3
import json


s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')
args = getResolvedOptions(sys.argv, ["JOB_NAME","WORKFLOW_NAME","WORKFLOW_RUN_ID"])
glue_client = boto3.client('glue')
workflow_properties = glue_client.get_workflow_run_properties(Name=args['WORKFLOW_NAME'],RunId=args['WORKFLOW_RUN_ID'])['RunProperties']
print(workflow_properties)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


s3_location = f"s3://test_bucket_12"
db_name=workflow_properties['TEST_DB']
s3_data_as_is = glueContext.create_dynamic_frame.from_options(connection_type="s3", connection_options={"paths": [s3_location]},   format="csv",    format_options={"withHeader": True,},)
s3_record_count = s3_data_as_is.count()
print(s3_record_count)

if s3_record_count == 0:
    print("No record found")
    job.commit()
    os._exit(0)
column_names = s3_data_as_is.toDF().columns
print(column_names)

def TrimValues(row):
  for column in column_names:
    row[column] = row[column].strip()
  return row

print(f"AS-IS S3 data read {s3_data_as_is.count()}")
s3_data_cleansed = s3_data_as_is.map(f = TrimValues)
print(f"Cleansed S3 data read {s3_data_cleansed.count()}")

expected_columns_ruleset = """
    Rules = [
    ColumnCount >= 33
    ]
"""
evaluate_quality_for_expected_number_of_columns = EvaluateDataQuality().process_rows(
    frame=s3_data_cleansed,
    ruleset=expected_columns_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "evaluate_quality_for_expected_number_of_columns",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)
expected_columns_rule_failures = evaluate_quality_for_expected_number_of_columns[
        EvaluateDataQuality.DATA_QUALITY_RULE_OUTCOMES_KEY
    ].filter(lambda x: x["Outcome"] == "Failed")
expected_columns_rule_failures.show()
assert (expected_columns_rule_failures.count() == 0), "The job failed due to failing DQ rules for node: EvaluateDataQuality_expected_columns"


s3_data_after_initial_transformation = ApplyMapping.apply(
    frame=s3_data_cleansed,
    mappings=[
        ("date", "string", "date", "string"),
        ("time", "string", "time", "string"),
        ("code", "string", "code", "string"),
        ("c_r", "string", "code", "string"),
        ("response", "string", "code", "string"),
    ],
    transformation_ctx="s3_data_after_initial_transformation",
)

postgre_reference_data_ref_test = glueContext.create_dynamic_frame.from_catalog(
    database=db_name,
    table_name=f"{db_name}_public_ref_test",
    transformation_ctx="ref_test",
)

data_quality_additional_sources = {
    "reftest": postgre_reference_data_ref_test
}

# Script generated for node Evaluate Data Quality
field_level_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
    ColumnValues "date" matches "^[0-9]{8}$",
    ColumnValues "time" matches "^[0-9]{6}$",    
    ColumnValues "code" matches "^[0-9]{13,19}$",    
    (ColumnValues "c_r" = "") or (ColumnValues "completion" matches "^(I|S|A)[0-9]{2}$"),
    ColumnValues "response" matches "^(T|[0-9])[0-9]{1}$",    
    (ColumnValues "c_r" = "") or (ReferentialIntegrity "c_r" "reftest.{ref_value}" = 1.0)
    ]
"""


evaluate_quality_for_field_level_ruleset = EvaluateDataQuality().process_rows(
    frame=s3_data_after_initial_transformation,
    additional_data_sources=data_quality_additional_sources,
    ruleset=field_level_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "evaluate_quality_for_field_level_ruleset",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

row_level_outcome_after_applying_field_level_ruleset = SelectFromCollection.apply(
    dfc=evaluate_quality_for_field_level_ruleset,
    key="rowLevelOutcomes",
    transformation_ctx="row_level_outcome_after_applying_field_level_ruleset",
)

failed_records_after_applying_field_level_ruleset = row_level_outcome_after_applying_field_level_ruleset.filter(
    f=lambda x: x["DataQualityEvaluationResult"] != "Passed")
failed_records_count = failed_records_after_applying_field_level_ruleset.count()
print(f"Failed Records count {failed_records_count}")
failed_records_after_applying_field_level_ruleset.select_fields(paths=["uniq_column", "DataQualityRulesFail"]).show()
assert (failed_records_count == 0), "The job failed due to failing DQ rules for node: row_level_outcome_after_applying_field_level_ruleset"
selected_data = s3_data_after_initial_transformation.select_fields(paths=["date","time","code"])
selected_data_record_count = selected_data.count()
print(f"SELECTED S3 data read {selected_data_record_count}")

def remove_leading_zeros(input_string):
    output_string = input_string.lstrip('0')
    return 0 if output_string == ''  else int(output_string)

def generate_hash(input_string):
    return hashlib.sha256(input_string.encode()).hexdigest()

def transform_data(row):     
  timestamp = datetime.strptime(row["date"] + ' ' + row["time"], '%Y%m%d %H%M%S')
  row["stimestamp"] = timestamp 
  row["sdate"] = datetime.strptime(row["date"] , '%Y%m%d').date()
  row["code_without_zero"] = remove_leading_zeros(row["code"])
  row["h_code"] = generate_hash(row["code"])
  row["code_2"] = row["code"][:2]
  row["code_24"] = row["code"][2:4]
  row["code_l2"] = row["code"][-2:]
  del row["date"]
  del row["time"]  
  return row
s3_transform_data = selected_data.map(f = transform_data)
s3_transform_data_record_count= s3_transform_data.count()
print(f"TRANSFORMED S3 data read {s3_transform_data_record_count}")

assert (selected_data_record_count == s3_transform_data_record_count), "The job failed due to mismatch in record count after data transformation"
biggest_date = s3_transform_data.select_fields(paths=["sdate"]).toDF().rdd.max()[0]
print(biggest_date)
glue_client.put_workflow_run_properties(Name=args["WORKFLOW_NAME"],RunId=args["WORKFLOW_RUN_ID"],RunProperties={'biggest_date': str(biggest_date)})

resp = glueContext.write_dynamic_frame_from_catalog(
frame=s3_transform_data,
database=db_name,
table_name=f"{db_name}_public_switch_txn",
transformation_ctx="resp",
additional_options=additionalOptions)

job.commit()
