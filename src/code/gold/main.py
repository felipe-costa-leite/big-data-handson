import sys
import logging

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# ============================================================
# LOGGING
# ============================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

stream = logging.StreamHandler(sys.stdout)
stream.setLevel(logging.INFO)
stream.setFormatter(logging.Formatter("[%(levelname)s]|%(asctime)s|%(message)s"))
logger.addHandler(stream)

logger.info("Iniciando pipeline EMR: silver -> gold (fact_daily_conversion)")

# ============================================================
# SPARK SESSION (Delta + Glue Data Catalog via enableHiveSupport)
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
    .appName("emr-gold-fact-daily-conversion")
    .config(conf=spark_config)
    .enableHiveSupport()
    .getOrCreate()
)

# ============================================================
# VARIÁVEIS DE CAMINHO / TABELAS
# ============================================================
bucket = "aws-s3-dados-data-lake"
database = "gold"

tabela_visitas = "silver.visitas"
tabela_pedidos = "silver.pedidos"
tabela_saida = f"{database}.fact_daily_conversion"

gold_path = f"s3://{bucket}/gold/fato_conversao_diaria"

exception_error = None

# ============================================================
# GARANTIR DATABASE GOLD NO CATÁLOGO
# ============================================================
try:
    logger.info(f"Garantindo existência do database '{database}' no catálogo.")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
except Exception as err:
    logger.exception("Erro ao criar/verificar database 'gold'.")
    exception_error = err

# ============================================================
# QUERY DE CONVERSÃO (mantida como no original)
# ============================================================
query_conversao = f"""
WITH visitas_por_dia AS (
  SELECT
    CAST(data_visita AS DATE) AS dt,
    COUNT(*)                    AS qtd_visitas_dia
  FROM {tabela_visitas}
  GROUP BY CAST(data_visita AS DATE)
),
pedidos_por_dia AS (
  SELECT
    CAST(data_pedido AS DATE) AS dt,
    COUNT(*)                  AS qtd_pedidos_dia,
    SUM(valor_total)          AS receita_total_dia
  FROM {tabela_pedidos}
  GROUP BY CAST(data_pedido AS DATE)
),
totais AS (
  SELECT
    SUM(v.qtd_visitas_dia) AS total_visitas,
    SUM(COALESCE(p.qtd_pedidos_dia, 0)) AS total_pedidos
  FROM visitas_por_dia v
  LEFT JOIN pedidos_por_dia p
    ON v.dt = p.dt
)
SELECT
  v.dt as event_date,
  v.qtd_visitas_dia as daily_visits,
  COALESCE(p.qtd_pedidos_dia, 0)   AS daily_orders,
  COALESCE(p.receita_total_dia, 0) AS daily_revenue,
  CASE
    WHEN COALESCE(p.qtd_pedidos_dia, 0) > 0
      THEN COALESCE(p.receita_total_dia, 0) / p.qtd_pedidos_dia
    ELSE 0
  END AS avg_order_value,
  CASE
    WHEN v.qtd_visitas_dia > 0
      THEN COALESCE(p.qtd_pedidos_dia, 0) * 1.0 / v.qtd_visitas_dia
    ELSE 0
  END AS daily_conversion_rate,
  CASE
    WHEN t.total_visitas > 0
      THEN t.total_pedidos * 1.0 / t.total_visitas
    ELSE 0
  END AS overall_conversion_rate,
  current_date() as inserted_at
FROM visitas_por_dia v
LEFT JOIN pedidos_por_dia p
  ON v.dt = p.dt
CROSS JOIN totais t
"""

# ============================================================
# EXECUÇÃO DA QUERY E GRAVAÇÃO EM DELTA
# ============================================================
if not exception_error:
    try:
        logger.info("Executando query de conversão para fact_daily_conversion.")
        df_conversao = spark.sql(query_conversao)

        logger.info(f"Gravando fact_daily_conversion em Delta na camada GOLD: {gold_path}")

        (
            df_conversao.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("inserted_at")
            .save(gold_path)
        )

        logger.info("Registrando tabela gold.fact_daily_conversion no catálogo usando LOCATION.")

        spark.sql(f"DROP TABLE IF EXISTS {tabela_saida}")
        spark.sql(f"""
            CREATE TABLE {tabela_saida}
            USING DELTA
            LOCATION '{gold_path}'
        """)

        logger.info("Silver -> Gold (fact_daily_conversion) concluído com sucesso.")

    except Exception as err:
        logger.exception("Erro no processamento silver -> gold (fact_daily_conversion).")
        exception_error = err

# ============================================================
# FINALIZAÇÃO
# ============================================================
if exception_error:
    logger.error("Erro detectado no pipeline gold, encerrando com falha.")
    raise exception_error

logger.info("Pipeline EMR silver -> gold finalizado com sucesso.")
spark.stop()
