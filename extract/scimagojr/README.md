# Extractor ScimagoJR

Este módulo se encarga de la extracción de datos de ranking de revistas desde ScimagoJR.

## Características
*   **Checkpoints**: Utiliza una colección en MongoDB (`etl_checkpoints`) para rastrear qué años ya han sido procesados.
*   **Idempotencia**: Si se vuelve a ejecutar un año, el sistema elimina los registros previos de ese año antes de insertar los nuevos, evitando duplicados.
*   **Normalización**: Convierte el formato CSV/XLS de ScimagoJR a documentos JSON listos para MongoDB.

## Uso
El extractor puede ser llamado desde un DAG de Airflow o como un script independiente.

```python
from extract.scimagojr.scimagojr_extractor import ScimagoJRExtractor
extractor = ScimagoJRExtractor(mongodb_uri, db_name)
extractor.run(1999, 2023)
```
