CREATE DATABASE IF NOT EXISTS iceberg ENGINE = DataLakeCatalog(
    'http://iceberg-rest:8181/v1'
)
SETTINGS
    catalog_type = 'rest',
    warehouse = 's3://warehouse/',
    storage_endpoint = 'http://minio:9000';
