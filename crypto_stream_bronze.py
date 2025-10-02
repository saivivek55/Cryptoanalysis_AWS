import sys, datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME','TempDir'])   # TempDir is auto-set by Glue Studio
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BOOTSTRAP = "XXX"   
API_KEY   = "XXX"
API_SECRET= "XXX"
TOPIC     = "real_time"
BRONZE_PATH = "XXX/"
CHECKPOINT = "XXX/"

# ----------------------
# Schema of Kafka messages
# ----------------------
crypto_schema = StructType([
    StructField("id", StringType()),
    StructField("symbol", StringType()),
    StructField("image", StringType()),
    StructField("price", DoubleType()),
    StructField("market_cap", LongType()),
    StructField("market_cap_rank", LongType()),
    StructField("volume", LongType()),
    StructField("high_24h", DoubleType()),
    StructField("low_24h", DoubleType()),
    StructField("price_change_24h", DoubleType()),
    StructField("price_change_percentage_24h", DoubleType()),
    StructField("circulating_supply", DoubleType()),
    StructField("total_supply", DoubleType()),
    StructField("max_supply", DoubleType()),
    StructField("ath", DoubleType()),
    StructField("ath_change_percentage", DoubleType()),
    StructField("ath_date", StringType()),
    StructField("atl", DoubleType()),
    StructField("atl_change_percentage", DoubleType()),
    StructField("atl_date", StringType()),
    StructField("last_updated", StringType())
])

# ----------------------
# Read from Kafka
# ----------------------
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("kafka.security.protocol", "SASL_SSL")
       .option("kafka.sasl.mechanism", "PLAIN")
       .option("kafka.sasl.jaas.config",
               f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";')
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")  # <-- Use earliest to replay old messages
       .option("failOnDataLoss", "false")
       .load())

# ----------------------
# Parse JSON messages
# ----------------------
bronze_df = (raw
    .select(F.from_json(F.col("value").cast("string"), crypto_schema).alias("data"),
            F.current_timestamp().alias("ingest_ts"),
            F.to_date(F.current_timestamp()).alias("ingest_date"))
    .select("data.*", "ingest_ts", "ingest_date")
)

# ----------------------
# Write to Bronze S3
# ----------------------
query = (bronze_df.writeStream
  .format("json")
  .option("path", BRONZE_PATH)
  .option("checkpointLocation", CHECKPOINT)
  .partitionBy("ingest_date")   # daily partition
  .outputMode("append")
  .start())

query.awaitTermination()
job.commit()