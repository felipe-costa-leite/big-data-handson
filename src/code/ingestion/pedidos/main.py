import csv
import os
import random
from datetime import datetime, timedelta
import boto3

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID_BIG_DATA")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY_BIG_DATA")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


BUCKET_NAME = "aws-s3-dados-data-lake"


DATA_HOJE = datetime.now().strftime("%Y-%m-%d")
S3_PREFIX = f"landing/batch/pedidos/{DATA_HOJE}/"
LOCAL_CSV_FILENAME = f"pedidos_{DATA_HOJE}.csv"

NUM_PEDIDOS_POR_DIA = 1000

NUM_DIAS = 5

CANAIS_VENDA = ["organico", "pago", "email", "social"]
CLIENTES_IDS = list(range(100, 500))


def gerar_pedido(pedido_id: int, data_pedido: datetime) -> dict:
    cliente_id = random.choice(CLIENTES_IDS)
    canal_venda = random.choice(CANAIS_VENDA)
    valor_total = round(random.uniform(30.0, 600.0), 2)

    return {
        "pedido_id": pedido_id,
        "data_pedido": data_pedido.strftime("%Y-%m-%d"),
        "cliente_id": cliente_id,
        "canal_venda": canal_venda,
        "valor_total": valor_total,
    }


def gerar_pedidos_csv(filename: str, num_pedidos_por_dia: int, num_dias: int):

    hoje = datetime.now().date()
    pedidos = []

    pedido_id = 1
    for i in range(num_dias):
        dia = hoje - timedelta(days=i)
        for _ in range(num_pedidos_por_dia):
            pedidos.append(
                gerar_pedido(
                    pedido_id,
                    datetime.combine(dia, datetime.min.time())
                )
            )
            pedido_id += 1

    pedidos = sorted(pedidos, key=lambda x: x["data_pedido"])

    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["pedido_id", "data_pedido", "cliente_id", "canal_venda", "valor_total"],
        )
        writer.writeheader()
        writer.writerows(pedidos)




def upload_para_s3(local_path: str, bucket: str, s3_prefix: str):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    s3_key = os.path.join(s3_prefix, os.path.basename(local_path)).replace("\\", "/")

    s3.upload_file(local_path, bucket, s3_key)


def main():
    gerar_pedidos_csv(LOCAL_CSV_FILENAME, NUM_PEDIDOS_POR_DIA, NUM_DIAS)
    upload_para_s3(LOCAL_CSV_FILENAME, BUCKET_NAME, S3_PREFIX)


if __name__ == "__main__":
    main()
