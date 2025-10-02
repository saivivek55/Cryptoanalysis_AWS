import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_now
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node = glueContext.create_dynamic_frame.from_catalog(database="crypto_bronze", table_name="bronze_tableingest_date_2025_09_10", transformation_ctx="AWSGlueDataCatalog_node")

# Script generated for node Change Schema
ChangeSchema_node1 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node, mappings=[("id", "string", "id", "string"), ("symbol", "string", "symbol", "string"), ("price", "double", "price", "double"), ("market_cap", "long", "market_cap", "long"), ("market_cap_rank", "int", "market_cap_rank", "int"), ("volume", "long", "volume", "long"), ("high_24h", "double", "high_24h", "double"), ("low_24h", "double", "low_24h", "double"), ("price_change_24h", "double", "price_change_24h", "double"), ("price_change_percentage_24h", "double", "price_change_percentage_24h", "double"), ("circulating_supply", "double", "circulating_supply", "double"), ("total_supply", "double", "total_supply", "double"), ("max_supply", "double", "max_supply", "double"), ("ath", "double", "ath", "double"), ("ath_change_percentage", "double", "ath_change_percentage", "double"), ("ath_date", "string", "ath_date", "string"), ("atl", "double", "atl", "double"), ("atl_change_percentage", "double", "atl_change_percentage", "double"), ("atl_date", "string", "atl_date", "string"), ("last_updated", "string", "last_updated", "timestamp"), ("ingest_ts", "string", "ingest_ts", "timestamp")], transformation_ctx="ChangeSchema_node1757453731270")

# Script generated for node Filter
Filter_nod = Filter.apply(frame=ChangeSchema_node, f=lambda row: (row["price"] >= 0 and row["market_cap"] >= 0), transformation_ctx="Filter_node")

# Script generated for node Drop Fields
DropFields_nod = DropFields.apply(frame=Filter_node, paths=["ath_date", "atl_date"], transformation_ctx="DropFields_node")

# Script generated for node Add Current Timestamp
AddCurrentTimestamp_node = DropFields_node.gs_now(colName="silver_loaded_at", dateFormat="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

# Script generated for node Amazon S3
if (AddCurrentTimestamp_node.count() >= 1):
   AddCurrentTimestamp_node = AddCurrentTimestamp_node.coalesce(1)
AmazonS3_node = glueContext.getSink(path="XXX", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1757475349123")
AmazonS3_nodev.setCatalogInfo(catalogDatabase="crypto_bronze",catalogTableName="Crypto_silver")
AmazonS3_node.setFormat("glueparquet", compression="snappy")
AmazonS3_node.writeFrame(AddCurrentTimestamp_node1)
job.commit()