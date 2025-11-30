import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_timestamp

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set(
    "spark.sql.catalog.spark_catalog",
    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
)

bucket = "aws-s3-dados-data-lake"

landing_pedidos_path = f"s3://{bucket}/landing/batch/pedidos/"
landing_visitas_path = f"s3://{bucket}/landing/streaming/visitas/"

bronze_pedidos_path = f"s3://{bucket}/bronze/pedidos"
bronze_visitas_path = f"s3://{bucket}/bronze/visitas"

checkpoint_visitas_path = f"s3://{bucket}/checkpoints/bronze/visitas/"
schema_visitas_path = f"s3://{bucket}/schemaLocation/bronze/visitas/"

df_pedidos_landing = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
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

df_visitas_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
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

job.commit()
