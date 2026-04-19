#!/usr/bin/env python3
# Script para generar datos de prueba en Kafka

from kafka import KafkaProducer
import random
import time
from datetime import datetime

# Conectar a Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Configuración
topic = 'movielog1'
num_mensajes = 200  # Genera 200 mensajes de prueba

print(f"🚀 Generando {num_mensajes} mensajes en topic '{topic}'...")

# Status codes y latencias realistas
status_codes = ['200', '200', '200', '200', '200', '200', '404', '500']  # 75% exitosos
latencias = [12, 15, 23, 34, 45, 67, 89, 123, 156, 234, 345, 456, 678, 890]

for i in range(num_mensajes):
    # Crear mensaje simulado
    timestamp = int(time.time())
    usuario = f"user{random.randint(1, 100)}"
    tipo = "recommendation request"
    status = random.choice(status_codes)
    latencia = random.choice(latencias)
    
    # Formato: timestamp,usuario,tipo,status,latencia ms
    mensaje = f"{timestamp},{usuario},{tipo},{status},{latencia} ms"
    
    # Enviar a Kafka
    producer.send(topic, mensaje.encode('utf-8'))
    
    if (i + 1) % 50 == 0:
        print(f"  ✓ {i + 1} mensajes enviados...")
    
    # Pequeña pausa para simular realismo
    time.sleep(0.05)

producer.flush()
print(f"✅ ¡Completado! {num_mensajes} mensajes generados en Kafka.")
print(f"📊 Distribución aproximada:")
print(f"   - Status 200 (OK): ~75%")
print(f"   - Status 404/500 (Error): ~25%")
print(f"   - Latencias: 12-890 ms")
