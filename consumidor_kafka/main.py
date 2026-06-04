import os
import json
import time
import requests
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
CACHE_URL = os.getenv("CACHE_URL", "http://cache_system:8000")
URL_METRICAS = "http://metricas:8000"

# Tópicos
TOPIC_PRINCIPAL = "consultas_geo"
TOPIC_REINTENTO = "consultas_reintento"
TOPIC_DLQ = "consultas_dlq"

GRUPO_CONSUMO = "grupo-consumidores-1"
MAX_INTENTOS = 3

def iniciar_kafka():
    print(f"⏳ Conectando a Kafka en {KAFKA_BROKER}...", flush=True)
    while True:
        try:
            # El consumidor escucha tanto el tópico principal como el de reintentos
            consumer = KafkaConsumer(
                TOPIC_PRINCIPAL,
                TOPIC_REINTENTO,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id=GRUPO_CONSUMO,
                auto_offset_reset='earliest'
            )
            
            # Necesitamos un productor aquí mismo para enviar a Reintento o DLQ
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Conectado a Kafka exitosamente.", flush=True)
            return consumer, producer
        except Exception as e:
            time.sleep(3)

def mandar_metrica(tipo, id_consulta):
    """Avisamos al sistema de métricas si reintentamos o mandamos a DLQ"""
    try:
        requests.post(f"{URL_METRICAS}/registrar", json={"evento": tipo, "datos": {"key": id_consulta, "t_res": 0}}, timeout=1)
    except:
        pass

if __name__ == "__main__":
    consumer, producer = iniciar_kafka()
    print(f"🎧 Consumidor activo. Escuchando tópicos...", flush=True)
    
    for mensaje in consumer:
        payload = mensaje.value
        id_consulta = payload.get("id_consulta")
        q = payload.get("query")
        z_a = payload.get("zona_a")
        params = payload.get("parametros", {})
        intentos = payload.get("intentos", 0)
        
        # 1. Construir la URL exacta que usabas en la Tarea 1
        if q == "q4":
            z_b = params.get("zona_b")
            url = f"{CACHE_URL}/consulta/{q}/{z_a}/{z_b}"
        else:
            url = f"{CACHE_URL}/consulta/{q}/{z_a}"
            
        req_params = {k: v for k, v in params.items() if k != "zona_b"}
        
        # 2. Intentar procesar la consulta
        try:
            res = requests.get(url, params=req_params, timeout=5)
            data = res.json()
            
            # Tu cache_system devuelve un JSON con "error" si el backend se cae
            if res.status_code == 200 and "error" not in data:
                print(f"✅ Éxito procesando {q} en {z_a} (Intento {intentos})", flush=True)
                # Ojo: Tu cache_system ya manda la métrica de HIT/MISS internamente.
            else:
                raise Exception(f"Backend reportó error o indisponibilidad: {data.get('error', 'Desconocido')}")
                
        except Exception as e:
            print(f"⚠️ Fallo procesando {q} en {z_a} - Error: {e}", flush=True)
            payload["intentos"] += 1
            
            # 3. Lógica de Fallback y DLQ
            if payload["intentos"] < MAX_INTENTOS:
                print(f"🔄 Reenviando a Tópico de Reintentos (Intento {payload['intentos']}/{MAX_INTENTOS})", flush=True)
                producer.send(TOPIC_REINTENTO, value=payload)
                mandar_metrica("retry", id_consulta)
            else:
                print(f"💀 Máximo de reintentos alcanzado. Enviando a DLQ.", flush=True)
                producer.send(TOPIC_DLQ, value=payload)
                mandar_metrica("dlq", id_consulta)
                
    producer.flush()
