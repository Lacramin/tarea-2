# Tarea 2: Sistemas Distribuidos 

Este repositorio contiene la implementación de la Tarea 2 del curso de Sistemas Distribuidos. El proyecto se centra en la simulación, generación de tráfico y procesamiento de mensajes utilizando una arquitectura basada en microservicios y Apache Kafka.

## Integrantes
* Benjamín Arango Reyes
* Jeremías Olivares
* Yhean Fernández

## Tecnologías Utilizadas
* **Python 3.x:** Lógica principal de generadores y consumidores.
* **Apache Kafka:** Broker de mensajería para el manejo del flujo de datos.
* **Docker & Docker Compose:** Contenerización y orquestación de los servicios.

## Estructura del Proyecto
El repositorio está organizado en los siguientes módulos principales:
* `/generador_trafico`: Script que simula el envío continuo de peticiones al sistema.
* `/generador_respuestas`: Módulo encargado de procesar y responder al tráfico generado.
* `/consumidor_kafka`: Servicio suscrito a los tópicos de Kafka para consumir y registrar los eventos.
* `docker-compose.yml`: Configuración para levantar el ecosistema (Kafka, Zookeeper, etc.).
* `run_experiments.sh`: Script automatizado para ejecutar las pruebas.
* `generar_graficos.py`: Herramienta para visualizar las métricas resultantes de los experimentos.

---

## Paso a Paso (Cómo ejecutarlo)

Sigue estas instrucciones para levantar el proyecto en tu entorno local.

### 1. Clonar el repositorio
Primero, descarga el código a tu máquina local:
```bash
git clone [https://github.com/Lacramin/tarea-2.git](https://github.com/Lacramin/tarea-2.git)
cd tarea-2
