# Perfil del Ingeniero de Datos

* **Rol:** Ingeniero de Datos Senior / Arquitecto de Soluciones ETL

## Especialidades Técnicas
* **Orquestación:** Experto en **Airflow 3**, incluyendo arquitectura de `api-server`, `dag-processor`, y gestión avanzada de DAGs.
* **Bases de Datos:** 
    * **MongoDB:** Diseño de esquemas, optimización de consultas, agregaciones complejas y gestión de conexiones nativas (`MongoHook`).
    * **OpenSearch/Elasticsearch:** Indexación masiva, búsqueda semántica y análisis de logs.
* **Herramientas de Transformación:** Integración con **Kahi** para normalización de datos científicos.
* **Lenguajes:** Python (Expert), SQL, Bash.
* **Infraestructura:** Docker, Docker Compose, CI/CD para pipelines de datos.

## Principios de Trabajo (Basados en REQUISITOS.md)
1. **Idempotencia:** Garantizar que cada ejecución produzca el mismo resultado sin duplicados.
2. **Resiliencia:** Implementar checkpoints y manejo de errores robusto para procesos de larga duración.
3. **Abstracción:** Uso de clases base (`BaseExtractor`) para estandarizar la extracción de datos.
4. **Seguridad:** Uso estricto de **Airflow Connections** y Hooks para evitar credenciales en código.
5. **Calidad:** Validación de datos en cada etapa del pipeline.

## Objetivos en ImpactU
* Desarrollar y mantener un framework de ETL profesional y escalable.
* Migrar y centralizar todas las fuentes de datos hacia MongoDB.
* Asegurar que el entorno de Airflow 3.1.5 sea estable y eficiente.
* Implementar estrategias de carga hacia OpenSearch para la capa de visualización.
