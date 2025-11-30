import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

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
database = "gold"
tabela_visitas = f"silver.visitas"
tabela_pedidos = f"silver.pedidos"
tabela_saida = f"{database}.fact_daily_conversion"

gold_path = f"s3://{bucket}/gold/fato_conversao_diaria"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

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


df_conversao = spark.sql(query_conversao)

(
    df_conversao.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("inserted_at")
    .saveAsTable(tabela_saida)
)

job.commit()
