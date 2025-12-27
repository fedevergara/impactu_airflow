# ImpactU Airflow ETL

Repositorio central de DAGs de Apache Airflow para los procesos de Extracción, Transformación y Carga (ETL) del proyecto ImpactU.

## 🚀 Descripción
Este proyecto orquestaliza la recolección de datos de diversas fuentes científicas y académicas, su procesamiento mediante la herramienta [Kahi](https://github.com/colav/Kahi) y su posterior carga en sistemas de consulta como MongoDB y Elasticsearch.

## 📂 Estructura del Proyecto
El repositorio está organizado por etapas del ciclo de vida del dato:

*   `extract/`: Lógica de extracción para fuentes como OpenAlex, ORCID, ROR, etc.
*   `transform/`: Procesos de transformación y normalización (Kahi).
*   `load/`: Scripts de carga hacia destinos finales.
*   `deploys/`: Configuraciones de despliegue por entorno (dev, prod).
*   `backups/`: Automatización de respaldos de bases de datos.
*   `tests/`: Pruebas de integración y calidad de datos.

## 📋 Requisitos y Arquitectura
Para detalles sobre los principios de diseño (Checkpoints, Idempotencia, Paralelismo), consulte el documento de [Requisitos del Sistema](REQUISITOS.md).

## 🛠 Estándar de Nombrado de DAGs
Para mantener la consistencia en la interfaz de Airflow, seguimos esta convención:

| Tipo | Formato | Ejemplo |
| :--- | :--- | :--- |
| **Extracción** | `extract_{fuente}` | `extract_openalex` |
| **Transformación** | `transform_{entidad}` | `transform_sources` |
| **Carga** | `load_{db}_{env}` | `load_mongodb_production` |
| **Despliegue** | `deploy_{servicio}_{env}` | `deploy_mongodb_production` |
| **Backup** | `backup_{db}_{nombre}` | `backup_mongodb_kahi` |
| **Pruebas** | `tests_{servicio}` | `tests_kahi` |

## ⚙️ Configuración y Desarrollo
*(Sección en construcción)*

### Requisitos Previos
*   Docker & Docker Compose
*   Apache Airflow 3.1.5
*   Python 3.12+

### Instalación
1. Clonar el repositorio.
2. Configurar las variables de entorno en un archivo `.env`.
3. Levantar el entorno con Docker Compose.

---
**Colav - ImpactU**
