# Tarea 2: Sistemas Distribuidos

Este repositorio contiene la implementación de la Tarea 2 del curso de Sistemas Distribuidos. El proyecto evoluciona una arquitectura originalmente síncrona hacia un modelo asíncrono y tolerante a fallos, centrado en la simulación, generación de tráfico y procesamiento de consultas geoespaciales utilizando Apache Kafka (KRaft).

## Integrantes

- Benjamín Arango Reyes
- Jeremías Olivares

## Tecnologías Utilizadas

- **Python 3.x:** Lógica principal de los microservicios.
- **FastAPI:** Creación de las APIs REST para el backend, caché y sistema de métricas.
- **Apache Kafka (KRaft):** Broker de mensajería para el encolado y orquestación asíncrona de datos, implementado sin Zookeeper para optimizar recursos de memoria.
- **Redis:** Sistema de almacenamiento en caché estructurado en memoria.
- **Pandas & NumPy:** Procesamiento y análisis del dataset geoespacial (`967_buildings.csv.gz`).
- **Docker & Docker Compose:** Contenerización, orquestación de servicios y escalamiento horizontal de réplicas.

## Estructura del Proyecto

El repositorio está organizado en los siguientes módulos principales:

- `/generador_trafico`: Actúa como **Kafka Producer**. Genera las consultas (Q1-Q5) empaquetadas con un ID único, un timestamp y las publica en el tópico principal.
- `/consumidor_kafka`: Microservicio suscrito a Kafka que actúa como el obrero de la arquitectura. Implementa el patrón *Cache-Aside*, gestiona los reintentos ante caídas del servidor y deriva los mensajes irrecuperables a una *Dead Letter Queue* (DLQ).
- `/cache_system`: Servicio intermediario que busca las consultas en Redis y delega las operaciones pesadas al backend en caso de un *miss*.
- `/generador_respuestas`: Backend encargado del análisis de datos en la ciudad. En esta versión incluye una lógica para inyectar fallos artificiales (40% de error simulado) y probar la resiliencia del ecosistema.
- `/metricas`: API centralizada que recibe y almacena los eventos (`hit`, `miss`, `retry`, `dlq`) en un registro unificado en tiempo real.
- `docker-compose.yml`: Archivo de orquestación configurado para levantar la infraestructura base (Kafka KRaft, Redis, APIs) en una misma red.
- `run_experiments.sh`: Script bash automatizado para desplegar el clúster, ejecutar los 5 escenarios de prueba exigidos (escalamiento, fallos, spikes de tráfico) y limpiar el entorno de forma desatendida.
- `generar_graficos.py`: Herramienta desarrollada con Matplotlib para procesar los CSV de resultados y generar gráficos circulares analíticos (Hit Rate, Retry Rate, DLQ Rate).

## Paso a Paso (Cómo ejecutarlo)

Sigue estas instrucciones para levantar el proyecto y reproducir los experimentos en tu entorno local.

1. **Clonar el repositorio** – Descarga el código a tu máquina local y accede a la carpeta del proyecto ejecutando `git clone https://github.com/TU_USUARIO/sistemas.git` y luego `cd sistemas`.

2. **Dar permisos de ejecución al script** – Asegúrate de que el script de automatización tenga los permisos necesarios para interactuar con Docker y el sistema de archivos usando `chmod +x run_experiments.sh`.

3. **Ejecutar la batería de pruebas** – El script orquesta toda la infraestructura, ejecuta los escenarios base, simula el escalamiento horizontal multiplicando consumidores, inyecta fallos para probar las colas de reintentos y descarga las métricas automáticamente. Lanza el proceso con `./run_experiments.sh` (toma alrededor de 8-10 minutos). Durante la ejecución verás en la consola cómo Docker levanta las imágenes, distribuye la carga y finalmente guarda los resultados `.csv` en la carpeta `/metricas`.

4. **Generar los gráficos de análisis** – Una vez finalizados los experimentos, crea las representaciones visuales ejecutando `python3 generar_graficos.py`. Los gráficos en formato `.png` (distribución del flujo y pérdidas) y un CSV resumen quedarán guardados en `/metricas/graficos_tarea2/`.

5. **Apagar y limpiar el entorno** – El script automatizado ya limpia el entorno tras cada prueba. Si levantaste los servicios manualmente con `docker compose up -d`, puedes detenerlo todo y liberar los puertos con `docker compose down -v`.
