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
bronze_visitas_path = f"s3://{bucket}/bronze/visitas"

silver_pedidos_path = f"s3://{bucket}/silver/pedidos"
silver_visitas_path = f"s3://{bucket}/silver/visitas"

exception_error = None

# ============================================================
# GARANTIR DATABASE NO CATÁLOGO (opcional, mas útil)
# ============================================================
try:
    logger.info("Garantindo existência do database 'silver' no catálogo.")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
except Exception as err:
    logger.exception("Erro ao criar/verificar database 'silver'.")
    exception_error = err

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
        logger.info(f"Lendo visitas da camada bronze: {bronze_visitas_path}")
        df_visitas_bronze = spark.read.format("delta").load(bronze_visitas_path)

        logger.info("Transformando visitas para camada silver.")

        df_visitas_silver = (
            df_visitas_bronze
            .dropDuplicates(["event_time", "cliente_id", "pagina", "session_id"])
            .withColumn(
                "data_visita",
                date_format(col("event_time"), "yyyy-MM-dd").cast("date")
            )
        )

        logger.info(f"Gravando visitas na camada silver (Delta): {silver_visitas_path}")

        (
            df_visitas_silver.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("data_visita")
            .save(silver_visitas_path)
        )

        logger.info("Registrando tabela silver.visitas no catálogo usando LOCATION.")

        spark.sql("DROP TABLE IF EXISTS silver.visitas")
        spark.sql(f"""
            CREATE TABLE silver.visitas
            USING DELTA
            LOCATION '{silver_visitas_path}'
        """)

        logger.info("Bronze -> Silver: visitas concluído com sucesso.")

    except Exception as err:
        logger.exception("Erro no processamento bronze -> silver para visitas.")
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
