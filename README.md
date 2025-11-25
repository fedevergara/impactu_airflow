# impactu_airflow
ImpactU Airflow Dags for ETL

# Información sobre el servicio

# Políticas de ejecución

TODO: definir forma trabajo

# Estandar de Nombrado DAG:
## Para extracción 
extract_db ej: extract_openalex

## Para transform
transform_entidad  ex: transform_sources  (se provee el yml con el flujo de sources a kahi)

## Para load
load_db_destination ex: load_mongodb_production, load_elasticsearch_production

## Para Deploy
deploy_service_destination  ex: deploy_mongodb_production, deploy_elasticsearch_development

## Para Backup
backup_db_source ex: backup_mongodb_kahi, backup_mongodb_impactu

## Para Tests 
tests_service ex: tests_kahi, tests_backend