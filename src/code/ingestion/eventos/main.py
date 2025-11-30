import boto3
import json
import random
import time
import os
from datetime import datetime, timezone, timedelta

FIREHOSE_STREAM_NAME = "aws-kinesis-firehose-eventos-handson-bigdata"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID_BIG_DATA")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY_BIG_DATA")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

client = boto3.client(
    "firehose",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

PAGINAS = ["/home", "/busca", "/produto/123", "/produto/456", "/carrinho", "/checkout"]
CANAIS = ["organico", "pago", "email", "social"]
DISPOSITIVOS = ["desktop", "mobile"]


def gerar_evento(event_time):
    evento = {
        "event_time": event_time,
        "cliente_id": random.randint(100, 999),
        "pagina": random.choice(PAGINAS),
        "canal": random.choice(CANAIS),
        "dispositivo": random.choice(DISPOSITIVOS),
        "session_id": f"sess-{random.randint(100000, 999999)}",
    }
    return evento


def enviar_evento(evento: dict):
    data_str = json.dumps(evento) + "\n"
    response = client.put_record(
        DeliveryStreamName=FIREHOSE_STREAM_NAME,
        Record={"Data": data_str.encode("utf-8")},
    )
    return response


def main():
    print(f"Enviando eventos para Firehose: {FIREHOSE_STREAM_NAME}")
    date_now = datetime.now()
    DATES = []
    for i in range(1, 6):
        DATES.append(
            date_now
        )
        date_now -= timedelta(days=1)

    for i in DATES:
        for k in range(0, 1000):
            evento = gerar_evento(event_time=i)
            enviar_evento(evento)
            print(f"Enviado: {evento}")
            time.sleep(1)


if __name__ == "__main__":
    main()
