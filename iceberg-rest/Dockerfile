FROM apache/iceberg-rest-fixture
USER root
RUN curl -fSL -o /usr/lib/iceberg-rest/postgresql-42.7.5.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.5.jar
USER iceberg
CMD ["java", "-cp", "iceberg-rest-adapter.jar:/usr/lib/iceberg-rest/postgresql-42.7.5.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
