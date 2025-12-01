import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import logging

# SETUP

logger = logging.Logger(__name__)
stream = logging.StreamHandler(sys.stdout)
stream.setLevel(logging.DEBUG)
stream.setFormatter(logging.Formatter("[%(levelname)s]|%(asctime)s|%(message)s"))
logger.addHandler(stream)

spark_config = SparkConf()

json_config = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.session.timeZone": "America/Sao_Paulo",
    "spark.databricks.delta.schema.autoMerge.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.databricks.delta.autoCompact.minNumFiles": "5"
}

for k, v in json_config.items():
    spark_config.set(key=k, value=v)

spark = (
    SparkSession.builder.config(conf=spark_config)
    .enableHiveSupport()
    .getOrCreate()
)


# VARS

bucket = "aws-s3-dados-data-lake"

landing_pedidos_path = f"s3://{bucket}/landing/batch/pedidos/"
landing_visitas_path = f"s3://{bucket}/landing/streaming/visitas/"

bronze_pedidos_path = f"s3://{bucket}/bronze/pedidos"
bronze_visitas_path = f"s3://{bucket}/bronze/visitas"

checkpoint_visitas_path = f"s3://{bucket}/landing/checkpoints/bronze/visitas/"
schema_visitas_path = f"s3://{bucket}/landing/schema/bronze/visitas/"

exception_error = ""

pedidos_schema = StructType([
    StructField("pedido_id", StringType(), True),
    StructField("data_pedido", StringType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("canal_venda", StringType(), True),
    StructField("valor_total", StringType(), True),
])

try:

    df_pedidos_landing = (
        spark.read
        .option("header", "true")
        .schema(pedidos_schema)
        .csv(landing_pedidos_path)
    )

    df_pedidos_bronze = (
        df_pedidos_landing
        .withColumn("valor_total", col("valor_total").cast("double"))
        .withColumn("data_pedido", col("data_pedido").cast("date"))
    )

    (
        df_pedidos_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(bronze_pedidos_path)
    )
except Exception as err:
    logger.error(err)
    exception_error = err

try:
    df_visitas_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_visitas_path)
        .load(landing_visitas_path)
    )

    df_visitas_bronze = (
        df_visitas_stream
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("cliente_id", col("cliente_id").cast("int"))
    )

    query = (
        df_visitas_bronze.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_visitas_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .start(bronze_visitas_path)
    )

    query.awaitTermination()
except Exception as err:
    logger.error(err)
    if not exception_error:
        exception_error = err
    raise exception_error
