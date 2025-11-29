import boto3
import json
import random
import time
from datetime import datetime, timezone

FIREHOSE_STREAM_NAME = "aws-kenissis"

client = boto3.client("firehose")

PAGINAS = ["/home", "/busca", "/produto/123", "/produto/456", "/carrinho", "/checkout"]
CANAIS = ["organico", "pago", "email", "social"]
DISPOSITIVOS = ["desktop", "mobile"]


def gerar_evento():
  agora = datetime.now(timezone.utc)
  evento = {
    "event_time": agora.isoformat(),
    "cliente_id": random.randint(100, 999),
    "pagina": random.choice(PAGINAS),
    "canal": random.choice(CANAIS),
    "dispositivo": random.choice(DISPOSITIVOS),
    "session_id": f"sess-{random.randint(100000, 999999)}"
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
  try:
    while True:
      evento = gerar_evento()
      enviar_evento(evento)
      print(f"Enviado: {evento}")
      time.sleep(0.5)  # 2 eventos por segundo (ajuste à vontade)
  except KeyboardInterrupt:
    print("Simulação encerrada.")


if __name__ == "__main__":
  main()
