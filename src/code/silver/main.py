import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    date_format,
)

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

bronze_pedidos_path = f"s3://{bucket}/bronze/pedidos"
bronze_visitas_path = f"s3://{bucket}/bronze/visitas"

silver_pedidos_path = f"s3://{bucket}/silver/pedidos"
silver_visitas_path = f"s3://{bucket}/silver/visitas"


print("### Lendo pedidos da camada bronze ###")
df_pedidos_bronze = spark.read.format("delta").load(bronze_pedidos_path)


df_pedidos_silver = (
    df_pedidos_bronze
    .dropDuplicates(["pedido_id"])
    .withColumn("valor_total", col("valor_total").cast("double"))
    .withColumn("data_pedido", col("data_pedido").cast("date"))
    .withColumn("ano", year(col("data_pedido")))
    .withColumn("mes", month(col("data_pedido")))
    .withColumn("dia", dayofmonth(col("data_pedido")))
)


(
    df_pedidos_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ano", "mes", "dia")
    .saveAsTable("silver.pedidos")
)



df_visitas_bronze = spark.read.format("delta").load(bronze_visitas_path)

df_visitas_silver = (
    df_visitas_bronze
    .dropDuplicates(["event_time", "cliente_id", "pagina", "session_id"])
    .withColumn(
        "data_visita",
        date_format(col("event_time"), "yyyy-MM-dd").cast("date")
    )
)

(
    df_visitas_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("data_visita")
    .saveAsTable("silver.visitas")
)

job.commit()
