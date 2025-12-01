import sys
import logging
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ============================================================
# LOGGING
# ============================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

stream = logging.StreamHandler(sys.stdout)
stream.setLevel(logging.INFO)
stream.setFormatter(logging.Formatter("[%(levelname)s]|%(asctime)s|%(message)s"))
logger.addHandler(stream)

logger.info("Iniciando pipeline EMR: landing -> bronze")

# ============================================================
# SPARK SESSION (Glue Data Catalog via enableHiveSupport)
# ============================================================
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
    SparkSession.builder
    .appName("emr-landing-to-bronze")
    .config(conf=spark_config)
    .enableHiveSupport()
    .getOrCreate()
)

# ============================================================
# VARIÁVEIS DE CAMINHO
# ============================================================
bucket = "aws-s3-dados-data-lake"

landing_pedidos_path = f"s3://{bucket}/landing/batch/pedidos/"
landing_visitas_path = f"s3://{bucket}/landing/streaming/visitas/"

bronze_pedidos_path = f"s3://{bucket}/bronze/pedidos"
bronze_visitas_path = f"s3://{bucket}/bronze/visitas"

checkpoint_visitas_path = f"s3://{bucket}/landing/checkpoints/bronze/visitas/"
schema_visitas_path = f"s3://{bucket}/landing/schema/bronze/visitas/"

exception_error = None

# ============================================================
# SCHEMAS
# ============================================================

pedidos_schema = StructType([
    StructField("pedido_id", StringType(), True),
    StructField("data_pedido", StringType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("canal_venda", StringType(), True),
    StructField("valor_total", StringType(), True),
])

visitas_schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("cliente_id", IntegerType(), True),
    StructField("pagina", StringType(), True),
    StructField("canal", StringType(), True),
    StructField("dispositivo", StringType(), True),
    StructField("session_id", StringType(), True),
])

# ============================================================
# GARANTIR DATABASE NO CATÁLOGO (opcional, mas útil)
# ============================================================
try:
    logger.info("Garantindo existência do database 'bronze' no catálogo.")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
except Exception as err:
    logger.exception("Erro ao criar/verificar database 'silver'.")
    exception_error = err

# ============================================================
# BATCH: PEDIDOS (CSV landing -> Delta bronze)
# ============================================================
try:
    logger.info(f"Lendo pedidos da landing: {landing_pedidos_path}")

    df_pedidos_landing = (
        spark.read
        .option("header", "true")
        .schema(pedidos_schema)
        .csv(landing_pedidos_path)
    )

    logger.info("Convertendo tipos para bronze (pedidos)")

    df_pedidos_bronze = (
        df_pedidos_landing
        .withColumn("valor_total", col("valor_total").cast("double"))
        .withColumn("data_pedido", col("data_pedido").cast("date"))
    )

    logger.info(f"Gravando pedidos em Delta na bronze: {bronze_pedidos_path}")

    (
        df_pedidos_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(bronze_pedidos_path)
    )

    spark.sql("DROP TABLE IF EXISTS bronze.pedidos")
    spark.sql(f"""
                CREATE TABLE bronze.pedidos
                USING DELTA
                LOCATION '{bronze_pedidos_path}'
            """)

    logger.info("Batch pedidos -> bronze concluído com sucesso.")

except Exception as err:
    logger.exception("Erro no processamento batch de pedidos.")
    exception_error = err

# ============================================================
# STREAMING (availableNow): VISITAS (JSON landing -> Delta bronze)
# ============================================================
try:
    logger.info(f"Inicializando streaming de visitas a partir de: {landing_visitas_path}")

    df_visitas_stream = (
        spark.readStream
        .format("json")
        .schema(visitas_schema)
        .load(landing_visitas_path)
    )

    df_visitas_bronze = (
        df_visitas_stream
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("cliente_id", col("cliente_id").cast("int"))
    )

    logger.info(
        f"Gravando visitas em Delta na bronze com availableNow, "
        f"checkpoint em {checkpoint_visitas_path}"
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

    spark.sql("DROP TABLE IF EXISTS bronze.visitas")
    spark.sql(f"""
                    CREATE TABLE bronze.visitas
                    USING DELTA
                    LOCATION '{bronze_visitas_path}'
                """)

    logger.info("Streaming visitas -> bronze (availableNow) concluído com sucesso.")

except Exception as err:
    logger.exception("Erro no streaming de visitas.")
    if not exception_error:
        exception_error = err

if exception_error:
    logger.error("Erro detectado no pipeline, encerrando com falha.")
    raise exception_error

logger.info("Pipeline EMR landing -> bronze finalizado com sucesso.")
spark.stop()
