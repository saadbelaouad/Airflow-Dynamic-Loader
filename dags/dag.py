from wsgiref.handlers import format_date_time

from airflow.decorators import dag
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.triggers.sql import SQLExecuteQueryTrigger
from airflow.providers.standard.example_dags.example_external_task_marker_dag import start_date
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import dag, task, get_current_context, task_group
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.models.param import Param
import requests, time, json

import requests
import duckdb
import json
import time

from airflow.operators.python import get_current_context

from airflow.sdk.definitions import context

URl_ALL_STATES = "https://opensky-network.org/api/states/all?extended=true"
DATA_FILE_NAME = '/opt/airflow/dags/data/data.json'



# You can wrap this in a PythonOperator to run in your DAG

# Configuration des endpoints
endpoint_to_params = {
    "states": {
        "url": "https://opensky-network.org/api/states/all?extended=true",
        "columns": [
            "icao24", "callsign", "origin_country", "time_position", "last_contact",
            "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
            "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
            "spi", "position_source", "category"
        ],
        "target_table": "bdd_airflow.main.openskynetwork_brute",
        "timestamp_required": False
    },
    "flights": {
        "url": "https://opensky-network.org/api/flights/all?begin={begin}&end={end}",
        "columns": [
            "icao24", "firstSeen", "estDepartureAirport", "lastSeen", "estArrivalAirport",
            "callsign", "estDepartureAirportHorizDistance", "estDepartureAirportVertDistance",
            "estArrivalAirportHorizDistance", "estArrivalAirportVertDistance",
            "departureAirportCandidatesCount", "arrivalAirportCandidatesCount"
        ],
        "target_table": "bdd_airflow.main.flights_brut",
        "timestamp_required": True
    }
}

def format_datetime(input_datetime):
    return input_datetime.strftime("%Y%m%d")

@task(multiple_outputs=True)
def run_parameters(params=None,dag_run=None):
    endpoint = params.get("endpoint", "states")
    start_date=format_datetime(dag_run.start_date)
    if endpoint not in endpoint_to_params:
        raise ValueError(f"Endpoint inconnu: {endpoint}")
    # Copier uniquement les champs simples
    config = endpoint_to_params[endpoint]
    out = {
        "endpoint": endpoint,
        "target_table": config["target_table"],
        "url": config["url"],
        "columns": config["columns"],
        "timestamp_required": config["timestamp_required"],
        "start_date":start_date
    }

    # Remplir l'URL pour les flights si nécessaire
    if out["timestamp_required"]:
        end = int(time.time())
        begin = end - 3600
        out["url"] = out["url"].format(begin=begin, end=end)

    return out



def states_to_dict(states_list, colonnes, timestamp):
    out = []
    for state in states_list:
        # Create a dictionary by zipping column names with row data
        state_dict = dict(zip(colonnes, state))
        state_dict['timestamp'] = timestamp
        out.append(state_dict)
    return out

def flights_to_dict(flights, timestamp):
    out = []
    for flight in flights:
        flight['timestamp'] = timestamp
        out.append(flight)
    return out

# Déballage automatique des DagParam
def unwrap(param):
    if hasattr(param, "value"):
        return unwrap(param.value)  # récursif si nested
    elif isinstance(param, list):
        return [unwrap(p) for p in param]
    elif isinstance(param, dict):
        return {k: unwrap(v) for k, v in param.items()}
    else:
        return param


@task(multiple_outputs=True)
def get_flight_data(ti=None):
    endpoint=ti.xcom_pull(task_ids="run_parameters", key="endpoint")
    """
    if params is None:
        params = context.get("params", {})
    """
    # Récupération du paramètre endpoint
    raw_endpoint = endpoint
    if raw_endpoint is None:
        raise ValueError("Veuillez fournir un paramètre 'endpoint' via le déclencheur")

    endpoint = str(raw_endpoint)
    print("Endpoint réel :", endpoint)

    # Récupération de la config
    config = endpoint_to_params.get(endpoint)
    if not config:
        raise ValueError(f"Aucune configuration trouvée pour l'endpoint '{endpoint}'")

    colonnes = config["columns"]
    url = config["url"]

    # Pour l'endpoint flights, ajouter begin et end
    if endpoint == "flights" and config.get("timestamp_required", False):
        end = int(time.time())
        begin = end - 3600  # dernière heure
        url = url.format(begin=begin, end=end)

    print("URL appelée :", url)

    # Requête API
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            raise Exception(f"Erreur API {resp.status_code}: {resp.text[:200]}")
        data = resp.json()
    except Exception as e:
        raise Exception(f"Erreur lors de la récupération des données depuis l'API: {e}")

    # Traitement selon la réponse
    if endpoint == "states":
        timestamp = data.get("time", int(time.time()))
        states_list = data.get("states", [])
        results_json = states_to_dict(states_list, colonnes, timestamp)
    elif endpoint == "flights":
        timestamp = int(time.time())
        if not isinstance(data, list):
            print("Réponse inattendue pour flights, retourne liste vide")
            results_json = []
        else:
            results_json = flights_to_dict(data, timestamp)
    else:
        raise ValueError(f"Endpoint inconnu: {endpoint}")
    date_start=ti.xcom_pull(task_ids="run_parameters", key="start_date")
    # Sauvegarde dans un fichier
    filename = f"dags/data/data_{date_start}.json"
    with open(filename, "w") as f:
        json.dump(results_json, f)

    print(f"Données sauvegardées dans {filename}, {len(results_json)} lignes")

    return {
        "filename": filename,
        "timestamp": timestamp,
        "rows": len(results_json)
    }


def check_tables():

    return SQLExecuteQueryOperator(
        task_id="check_tables",
        conn_id="DUCK_DB",
        sql="SHOW TABLES;",
        show_return_value_in_logs=True,
)


@task
def load_from_file(ti=None):
    # Récupérer les infos de fichier et endpoint depuis XCom
    params = ti.xcom_pull(task_ids='run_parameters')
    endpoint = params["endpoint"]
    target_table = params["target_table"]

    data_file_name = ti.xcom_pull(task_ids='get_flight_data', key='filename')
    timestamp = ti.xcom_pull(task_ids='get_flight_data', key='timestamp')

    conn = duckdb.connect("/opt/airflow/dags/data/bdd_airflow.duckdb")

    # Création des tables selon l'endpoint
    if endpoint == "states":
        conn.execute("""
        CREATE TABLE IF NOT EXISTS openskynetwork_brute (
            icao24 VARCHAR,
            callsign VARCHAR,
            origin_country VARCHAR,
            time_position BIGINT,
            last_contact BIGINT,
            longitude DOUBLE,
            latitude DOUBLE,
            baro_altitude DOUBLE,
            on_ground BOOLEAN,
            velocity DOUBLE,
            true_track DOUBLE,
            vertical_rate DOUBLE,
            sensors VARCHAR,
            geo_altitude DOUBLE,
            squawk VARCHAR,
            spi BOOLEAN,
            position_source INTEGER,
            category INTEGER,
            timestamp BIGINT
        );
        """)
    elif endpoint == "flights":
        conn.execute("""
        CREATE TABLE IF NOT EXISTS flights (
            icao24 VARCHAR,
            callsign VARCHAR,
            origin_country VARCHAR,
            time_position BIGINT,
            last_contact BIGINT,
            longitude DOUBLE,
            latitude DOUBLE,
            baro_altitude DOUBLE,
            on_ground BOOLEAN,
            velocity DOUBLE,
            true_track DOUBLE,
            vertical_rate DOUBLE,
            sensors VARCHAR,
            geo_altitude DOUBLE,
            squawk VARCHAR,
            spi BOOLEAN,
            position_source INTEGER,
            timestamp BIGINT
        );
        """)

    # Insérer les données depuis le fichier JSON existant
    conn.execute(f"""
    INSERT INTO {target_table}
    SELECT *
    FROM read_json_auto('{data_file_name}');
    """)

    conn.close()
    print(f"Données insérées dans {target_table} depuis {data_file_name}")




@task()
def nombre_rows(ti=None):
    timestamp = ti.xcom_pull(task_ids='get_flight_data', key='timestamp')
    nombre_ligne_attendus = ti.xcom_pull(task_ids='get_flight_data', key='rows')
    target_table = ti.xcom_pull(task_ids='run_parameters', key='target_table')
    nombre_ligne_trouve = 0
    try:
        conn = duckdb.connect("/opt/airflow/dags/data/bdd_airflow.duckdb")
        nombre_ligne_trouve = conn.execute(
            f'SELECT count(*) FROM {target_table} WHERE timestamp={timestamp}'
        ).fetchone()[0]
    finally:
        if conn:
            conn.close()

    if nombre_ligne_trouve != nombre_ligne_attendus:
        raise Exception(
            f"Not the same nombre in the API: attendu={nombre_ligne_attendus}, trouvé={nombre_ligne_trouve}"
        )


@task()
def check_duplicates(ti=None):

    # Récupère le nom de la table depuis XCom
    target_table = ti.xcom_pull(task_ids='run_parameters', key='target_table')
    if not target_table:
        raise ValueError("target_table est None, vérifie run_parameters")
    conn = duckdb.connect("/opt/airflow/dags/data/bdd_airflow.duckdb")
    # SQL correctement formaté pour DuckDB
    conn.execute(f"""
    SELECT callsign, timestamp, COUNT(*) AS cnt
    FROM {target_table}
    GROUP BY 1, 2
    HAVING cnt > 1;
    """)
    conn.close()

@task_group
def data_quality():

    nombre_rows()
    check_duplicates()


@dag(
    params={
        "endpoint": Param(default="states", enum=list(endpoint_to_params.keys())),
        "row_threshold": Param(default=600, type="integer"),
    },
    start_date=datetime(2025, 12, 23),
    schedule="16 13 * * *",
    catchup=True,
    max_active_runs=1,
)


def flights_pipeline():
    start = EmptyOperator(task_id="start")
    sensor_task = FileSensor(
        task_id="attendre_les_donnees",
        fs_conn_id="connection_fichier",
        #filepath="dags/new_data/{{params.endpoint}}.json", name of the file : the endpoint choosen
        filepath="dags/new_data/",
        poke_interval=120,
        mode="reschedule",
        timeout=600
    )
    params_task = run_parameters()
    flight_data_task = get_flight_data()
    load_data_task = load_from_file()
    check_tables_task = check_tables()
    data_quality_task = data_quality()
    end = EmptyOperator(task_id="end")
    start >> sensor_task >> params_task >> flight_data_task >> check_tables_task>> load_data_task >> data_quality_task >> end

flight_pipeline_dag = flights_pipeline()
