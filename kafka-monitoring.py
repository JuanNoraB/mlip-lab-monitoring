from kafka import KafkaConsumer  # Clase para LEER mensajes de Kafka
from prometheus_client import Counter, Histogram, start_http_server
# Counter: métrica que solo SUBE (para contar requests)
# Histogram: métrica para distribuciones (para latencias)
# start_http_server: expone métricas en un puerto HTTP

topic = 'movielog1'  # TOPIC = "canal" de Kafka donde están los logs

start_http_server(8765)  # Inicia servidor en puerto 8765 para exponer métricas
# Prometheus leerá de http://localhost:8765/metrics cada 5 seg

# Metrics like Counter, Gauge, Histogram, Summaries
# Refer https://prometheus.io/docs/concepts/metric_types/ for details of each metric

# MÉTRICA 1: Contador de requests por código HTTP
REQUEST_COUNT = Counter(
    'request_count_total',                # Nombre de la métrica
    'Recommendation Request Count',       # Descripción
    ['http_status']                       # Label: separa por status (200, 404, etc.)
)
# Resultado: request_count_total{http_status="200"} 150
#            request_count_total{http_status="404"} 5

# MÉTRICA 2: Histograma de latencias (tiempos de respuesta)
REQUEST_LATENCY = Histogram(
    'request_latency_seconds',            # Nombre de la métrica
    'Request latency (seconds)',          # Descripción
    buckets=(0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10)
)
# buckets: agrupa latencias en rangos (≤5ms, ≤10ms, ≤20ms, etc.)
# Permite calcular percentiles (p50, p95, p99)

def main():
    # CONSUMER: Objeto que lee mensajes de Kafka
    consumer = KafkaConsumer(
        topic,                                  # Lee del topic 'movielog1'
        bootstrap_servers='localhost:9092',     # Dirección del broker Kafka
        auto_offset_reset='latest',             # 'latest' = solo mensajes NUEVOS (no histórico)
        group_id=topic,                         # ID del grupo (guarda posición de lectura)
        enable_auto_commit=True,                # Auto-confirma mensajes leídos
        auto_commit_interval_ms=1000            # Confirma cada 1 segundo
    )

    # LOOP INFINITO: Espera mensajes nuevos continuamente
    for message in consumer:
        # message.value son BYTES, los convertimos a STRING
        event = message.value.decode('utf-8')
        # Ejemplo: "1642377600,user456,recommendation request,200,45.2 ms"
        
        values = event.split(',')  # Separa por comas en un array
        # values[0] = timestamp, values[1] = user, values[2] = tipo,
        # values[3] = status HTTP, values[4] = latencia
        
        if 'recommendation request' in values[2]:  # Solo procesa recomendaciones
            # EXTRAE el código HTTP (200, 404, 500, etc.)
            status = values[3]
            
            # INCREMENTA el contador para ese status específico
            REQUEST_COUNT.labels(http_status=status).inc()
            # Si status="200" → sube contador de 200s en 1
            # Si status="404" → sube contador de 404s en 1

            # EXTRAE latencia: "45.2 ms" → toma "45.2" → convierte a float
            time_taken = float(values[-1].strip().split(" ")[0])
            
            # REGISTRA en histograma (convierte ms a segundos: 45.2/1000 = 0.0452)
            REQUEST_LATENCY.observe(time_taken / 1000)

if __name__ == "__main__":
    main()  # Ejecuta loop infinito, presiona Ctrl+C para detener
