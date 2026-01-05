# Airflow Dynamic Loader

## Description
**Airflow Dynamic Loader** is a full-featured Airflow project that automates loading data from multiple APIs into a DuckDB warehouse.

It includes:
- Dynamic configuration of endpoints (`states` and `flights`)
- Fetching, transforming, and saving API data as JSON
- Conditional loading strategy based on dataset size (direct insert or file-based load)
- Automatic creation of DuckDB tables with appropriate schema
- Data quality verification (row counts and duplicates)
- Full Airflow DAG orchestration with task groups and branching

This project demonstrates **end-to-end data ingestion**, **Airflow branching**, and **data validation** for real-time API sources.

## Features
- **Multiple endpoints:** supports OpenSky `states` and `flights` APIs
- **Dynamic parameters:** endpoint, timestamp, and row threshold handled automatically
- **Data transformation:** maps API JSON data into a structured format for DuckDB
- **File-based intermediate storage:** JSON files saved with timestamps for larger datasets
- **Conditional loading:** automatically selects between direct SQL insert or file load based on row count
- **DuckDB table management:** creates tables dynamically if they don’t exist
- **Data quality checks:** validates row counts and detects duplicates using task groups
- **Full Airflow DAG orchestration:** includes start/end tasks, sensors, branching, and task groups

## DAG Structure
- **run_parameters** – prepares API parameters, endpoint info, target tables, and timestamp
- **get_flight_data** – fetches API data, transforms it, and saves JSON files
- **choose_loading_method** – branches workflow based on row count threshold
- **load_with_insert** – inserts small datasets directly into DuckDB
- **load_from_file** – loads large datasets from JSON into DuckDB
- **data_quality** – task group performing:
  - `nombre_rows` → verifies expected vs inserted row count
  - `check_duplicates` → checks for duplicates in the target table
- **start / end tasks** – marks the DAG start and completion
- **FileSensor** – optionally waits for external files (currently placeholder in the DAG)

## Technologies
- **Apache Airflow** – workflow orchestration and branching
- **DuckDB** – lightweight, local data warehouse
- **Python** – API calls, JSON processing, SQL inserts
- **Requests** – interacting with external APIs
