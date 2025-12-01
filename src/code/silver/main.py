import sys
import logging

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    date_format,
)

# ============================================================
# LOGGING
# ============================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

stream = logging.StreamHandler(sys.stdout)
stream.setLevel(logging.INFO)
stream.setFormatter(logging.Formatter("[%(levelname)s]|%(asctime)s|%(message)s"))
logger.addHandler(stream)

logger.info("Iniciando pipeline EMR: bronze -> silver")

# ============================================================
# SPARK SESSION (Delta + Glue Data Catalog via enableHiveSupport)
# ============================================================
spark_config = SparkConf()

json_config = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.session.timeZone": "America/Sao_Paulo"
}

for k, v in json_config.items():
    spark_config.set(key=k, value=v)

spark = (
    SparkSession.builder
    .appName("emr-bronze-to-silver")
    .config(conf=spark_config)
    .enableHiveSupport()
    .getOrCreate()
)

# ============================================================
# VARIÁVEIS DE CAMINHO
# ============================================================
bucket = "aws-s3-dados-data-lake"

bronze_pedidos_path = f"s3://{bucket}/bronze/pedidos"
bronze_eventos_path = f"s3://{bucket}/bronze/eventos"

silver_pedidos_path = f"s3://{bucket}/silver/pedidos"
silver_eventos_path = f"s3://{bucket}/silver/eventos"

exception_error = None

# ============================================================
# PEDIDOS: BRONZE -> SILVER
# ============================================================
if not exception_error:
    try:
        logger.info(f"Lendo pedidos da camada bronze: {bronze_pedidos_path}")
        df_pedidos_bronze = spark.read.format("delta").load(bronze_pedidos_path)

        logger.info("Transformando pedidos para camada silver.")

        df_pedidos_silver = (
            df_pedidos_bronze
            .dropDuplicates(["pedido_id"])
            .withColumn("valor_total", col("valor_total").cast("double"))
            .withColumn("data_pedido", col("data_pedido").cast("date"))
            .withColumn("ano", year(col("data_pedido")))
            .withColumn("mes", month(col("data_pedido")))
            .withColumn("dia", dayofmonth(col("data_pedido")))
        )

        logger.info(f"Gravando pedidos na camada silver (Delta): {silver_pedidos_path}")

        (
            df_pedidos_silver.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("ano", "mes", "dia")
            .save(silver_pedidos_path)
        )

        logger.info("Registrando tabela silver.pedidos no catálogo usando LOCATION.")

        spark.sql("DROP TABLE IF EXISTS silver.pedidos")
        spark.sql(f"""
            CREATE TABLE silver.pedidos
            USING DELTA
            LOCATION '{silver_pedidos_path}'
        """)

        logger.info("Bronze -> Silver: pedidos concluído com sucesso.")

    except Exception as err:
        logger.exception("Erro no processamento bronze -> silver para pedidos.")
        exception_error = err

# ============================================================
# VISITAS: BRONZE -> SILVER
# ============================================================
if not exception_error:
    try:
        logger.info(f"Lendo eventos da camada bronze: {bronze_eventos_path}")
        df_eventos_bronze = spark.read.format("delta").load(bronze_eventos_path)

        logger.info("Transformando eventos para camada silver.")

        df_eventos_silver = (
            df_eventos_bronze
            .dropDuplicates(["event_time", "cliente_id", "pagina", "session_id"])
            .withColumn(
                "data_evento",
                date_format(col("event_time"), "yyyy-MM-dd").cast("date")
            )
        )

        logger.info(f"Gravando eventos na camada silver (Delta): {silver_eventos_path}")

        (
            df_eventos_silver.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("data_evento")
            .save(silver_eventos_path)
        )

        logger.info("Registrando tabela silver.eventos no catálogo usando LOCATION.")

        logger.info("Bronze -> Silver: eventos concluído com sucesso.")

    except Exception as err:
        logger.exception("Erro no processamento bronze -> silver para eventos.")
        if not exception_error:
            exception_error = err

# ============================================================
# FINALIZAÇÃO
# ============================================================
if exception_error:
    logger.error("Erro detectado no pipeline bronze -> silver, encerrando com falha.")
    raise exception_error

logger.info("Pipeline EMR bronze -> silver finalizado com sucesso.")
spark.stop()
