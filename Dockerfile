FROM apache/airflow:3.1.1

USER airflow

# Install DuckDB package and Airflow DuckDB provider
RUN pip install --no-cache-dir \
    duckdb==1.1.1 \
    apache-airflow-providers-duckdb
