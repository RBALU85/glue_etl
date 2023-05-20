import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import transform_function
from awsglue.dynamicframe import DynamicFrame
import json
import boto3

# Parse Parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME", "job_config_file_loc"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Expecting S3 location of Job Config File
job_config_file_loc = args["job_config_file_loc"]

# Get bucket Name & Key
bucket_key = job_config_file_loc[len("s3://") :]
bucket_name, key = bucket_key.split("/", 1)

# Read Job config from S3
s3 = boto3.client('s3')

data = s3.get_object(Bucket=bucket_name, Key=key)
contents = data['Body'].read()
job_parameters = json.loads(contents.decode("utf-8"))

# Read source file
# source parameters needed
# S3 file path - this is the source file path
# file format - This ETL job supports only csv/parquet/avro files
# format_option - this depends on the type of file.
# Create Source Dynamic Frame
source_dataframe_dict = {}
for sourcedf_map_item in job_parameters["input"]["dataframes"]:
    if sourcedf_map_item["format"] == "csv":
        source_dynamicFrame = glueContext.create_dynamic_frame.from_options(
            connection_type='s3',
            connection_options={'paths'
                                : [sourcedf_map_item["location"]]},
            format=sourcedf_map_item["format"],
            format_options={'withHeader': sourcedf_map_item["csv_header"],
                            'separator': sourcedf_map_item["csv_delimiter"],
                            'quoteChar': sourcedf_map_item["csv_quote"]
            }
        )
    elif sourcedf_map_item["format"] == "parquet":
        source_dynamicFrame = glueContext.create_dynamic_frame.from_options(
            connection_type='s3',
            connection_options={'paths'
                                : [sourcedf_map_item["location"]]},
            format=sourcedf_map_item["format"]

        )
    else:
        # Glue Catalog Reference
        source_dynamicFrame = glueContext.create_dynamic_frame.from_catalog(
            database=sourcedf_map_item["glue_catalog_reference"]["database_name"],
            table_name=sourcedf_map_item["glue_catalog_reference"]["table_name"]
        )

    source_dataframe_dict[sourcedf_map_item["name"]] = source_dynamicFrame.toDF()

destination_dataframe_dict = transform_function.transformation(source_dataframe_dict, spark)

# Convert Source Dynamic Frame to Spark DataFrame
for destination_df in destination_dataframe_dict:
    new_dynamicFrame = DynamicFrame.fromDF(destination_dataframe_dict[destination_df], glueContext, destination_df)
    print("new_dynamicFrame created successfully")
    for destinationdf_map_item in job_parameters["output"]["dataframes"]:
        if destinationdf_map_item["name"] == destination_df:
            sink = glueContext.getSink(
                connection_type="s3",
                path=destinationdf_map_item["location"],
                enableUpdateCatalog=True)
            sink.setFormat(destinationdf_map_item["format"])
            sink.setCatalogInfo(catalogDatabase=destinationdf_map_item["glue_catalog_reference"]["database_name"], catalogTableName=destinationdf_map_item["glue_catalog_reference"]["table_name"])
            sink.writeFrame(new_dynamicFrame)
job.commit()
