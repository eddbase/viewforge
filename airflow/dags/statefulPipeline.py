import json
import re
import math
from functools import reduce
import pendulum
from datetime import timedelta, date, datetime

from airflow.sdk import DAG, task, Variable
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# =============================================================================
# SECTION 1: METADATA LAYER 
# =============================================================================
SPARK_JARS_PATH = "/home/moham/spark-3.5.5-bin-hadoop3/jars/postgresql-42.7.5.jar"
POSTGRES_CONNECTION = "jdbc:postgresql://localhost:5432/tpch" 
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "zxcv"
# ++
DSL_METADATA = {
    "catalogs": {
         "my_nessie": {
            "type": "iceberg",
            "sub_type": "nessie",
            "config": {
                "uri": "nessie_url",
                "warehouse": "s3a://warehouse"
            }
        },
        "pg_catalog": {
            "type": "database",
            "config": {
                "url": POSTGRES_CONNECTION,
                "driver": "org.postgresql.Driver",
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD
            }
        }
    },
    "sources": {
        "OrderStream": {"catalog": "pg_catalog", "table": "Orders"},
        "LineitemStream": {"catalog": "pg_catalog", "table": "Lineitem"},
        "Customer": {"catalog": "pg_catalog", "table": "Customer"}
    },
    "views": {
        "OrderCustomer": {
            "target_lag": "2 minute",
            "depends_on": [],
            "dataset_uri": "view://ordercustomer",
            "query": """
                SELECT O.o_orderkey, O.o_orderdate, O.o_shippriority, COUNT(*) AS cnt
                FROM OrderStream AS O JOIN Customer AS C
                  ON O.o_custkey = C.c_custkey
               WHERE C.c_mktsegment = 'BUILDING'
                 AND O.o_orderdate < DATE('1995-03-15')
            GROUP BY O.o_orderkey, O.o_orderdate, O.o_shippriority
            """,
            "materialize": {"type": "xcom"}
        },
        "TPCH_1": {
            "target_lag": "3 minute",
            "depends_on": ["OrderCustomer"],
            "dataset_uri": "view://tpch3",
            "query": """
              SELECT OC.o_orderkey, OC.o_orderdate, OC.o_shippriority,
                     SUM(cnt * L.l_extendedprice * (1 - L.l_discount)) AS revenue
                FROM LineitemStream AS L JOIN OrderCustomer AS OC
                  ON L.l_orderkey = OC.o_orderkey
               WHERE L.l_shipdate > DATE('1995-03-15')
            GROUP BY OC.o_orderkey, OC.o_orderdate, OC.o_shippriority
            """,
            "materialize": {"type": "sink", "catalog": "pg_catalog", "table": "TPCH_1"}
        }
    }
}

# =============================================================================
# SECTION 2: GENERIC EXECUTION ENGINE
# =============================================================================

def _get_spark_session(jar_path=SPARK_JARS_PATH):
    """Initializes and returns a Spark session with required configurations."""
    from pyspark.sql import SparkSession
    return SparkSession.builder \
      .appName("GeneratedSparkPipeline") \
      .config("spark.jars", jar_path) \
      .getOrCreate()

def _parse_target_lag(lag_string: str) -> dict:
    """Parses a human-readable lag string into seconds and a timedelta object."""
    match = re.match(r"(\d+)\s+(minute|hour|day)s?", lag_string, re.IGNORECASE)
    if not match:
        raise ValueError(f"Invalid TARGET_LAG format: '{lag_string}'")
    value = int(match.group(1))
    unit = match.group(2).lower()
    td = timedelta(**{unit + 's': value})
    return {'timedelta': td, 'seconds': int(td.total_seconds())}

def _calculate_gcd_for_list(numbers: list[int]) -> int:
    """Helper function to calculate the Greatest Common Divisor (GCD) for a list of numbers."""
    if not numbers:
        return 0
    return reduce(math.gcd, numbers)

def _convert_spark_rows_to_json_serializable(rows_as_dicts: list) -> list:
    """
    Converts a list of dictionaries (from Spark rows) to a JSON-serializable list.
    It specifically handles converting date/datetime objects to ISO 8601 strings.
    """
    serializable_list = []
    for row_dict in rows_as_dicts:
        serializable_row = {}
        for key, value in row_dict.items():
            if isinstance(value, (date, datetime)):
                serializable_row[key] = value.isoformat()
            else:
                serializable_row[key] = value
        serializable_list.append(serializable_row)
    return serializable_list

@task
def execute_view_logic_stateful(view_name: str, dag_id: str, base_refresh_rate_seconds: int, **kwargs):
    """
    A stateful function that uses Airflow Variables to manage state and decides whether to run or skip.
    The read-check-increment-write logic is now atomic to prevent race conditions.
    """
    runtime_variable_key = f"runtime_info_{dag_id}"
    print(f"Executing logic for view: {view_name}")

    try:
        runtime_info = Variable.get(runtime_variable_key, deserialize_json=True)
    except Exception as e:
        raise AirflowSkipException(f"Could not retrieve runtime info '{runtime_variable_key}'. Skipping. Error: {e}")

    # --- 1. Atomically check schedule, and update counter ---
    view_info = runtime_info[view_name]
    counter = view_info.get("counter", 0)
    view_refresh_rate = max(1, view_info["target_lag_seconds"] // base_refresh_rate_seconds)

    print(f"View '{view_name}': Current Counter={counter}, RefreshRate={view_refresh_rate}")
    
    # Decide if we should run based on the counter
    should_run = (counter % view_refresh_rate == 0)

    # *** FIX ***: Increment the counter and save state immediately, *before* any long-running work.
    # This makes the "check and increment" operation atomic for each task run.
    runtime_info[view_name]["counter"] += 1
    Variable.set(runtime_variable_key, runtime_info, serialize_json=True)
    print(f"Atomically incremented counter to {runtime_info[view_name]['counter']} and saved state.")

    if not should_run:
        skip_message = f"Skipping '{view_name}'. Condition not met for counter {counter}."
        print(skip_message)
        raise AirflowSkipException(skip_message)

    print(f"Executing '{view_name}'. Condition met for counter {counter}.")
    
    # --- 2. Execute Spark Logic ---
    try:
        spark = _get_spark_session()
        view_meta = DSL_METADATA["views"][view_name]
        query_string = view_meta["query"]

        # Load Dependencies
        for dep_view_name in view_meta.get("depends_on", []):
            data_variable_key = f"data_payload_{dag_id}_{dep_view_name}"
            print(f"Loading dependency '{dep_view_name}' from Variable: '{data_variable_key}'")
            try:
                data = Variable.get(data_variable_key, deserialize_json=True)
                if data:
                    spark.createDataFrame(data).createOrReplaceTempView(dep_view_name)
                else:
                    raise AirflowSkipException(f"Dependency '{dep_view_name}' has no data payload yet. Skipping.")
            except Exception as e:
                raise AirflowSkipException(f"Failed to load dependency '{dep_view_name}' from Variable. Error: {e}")

        # Load source tables
        for source_name, source_meta in DSL_METADATA["sources"].items():
            if re.search(r'\b' + re.escape(source_name) + r'\b', query_string):
                catalog_meta = DSL_METADATA["catalogs"][source_meta["catalog"]]
                if catalog_meta["type"] == "database":
                    conn_props = catalog_meta["config"]
                    df = spark.read.jdbc(url=conn_props["url"], table=source_meta["table"], properties={k: v for k, v in conn_props.items() if k not in ['url', 'type', 'driver']})
                    df.createOrReplaceTempView(source_name)

        # Execute Query
        print(f"Running query for '{view_name}':\n{query_string}")
        result_df = spark.sql(query_string)

        # Materialize Result
        materialize_config = view_meta["materialize"]
        if materialize_config["type"] == "sink":
            catalog_name = materialize_config["catalog"]
            table_name = materialize_config["table"]
            catalog_meta = DSL_METADATA["catalogs"][catalog_name]
            conn_props = catalog_meta["config"]
            print(f"Writing results to sink: {catalog_name}.{table_name}")
            result_df.write.jdbc(url=conn_props["url"], table=table_name, mode="overwrite", properties={k: v for k, v in conn_props.items() if k not in ['url', 'type', 'driver']})
        elif materialize_config["type"] == "xcom":
            data_variable_key = f"data_payload_{dag_id}_{view_name}"
            print(f"Persisting results to Variable: '{data_variable_key}'")
            collected_rows_as_dicts = [row.asDict() for row in result_df.collect()]
            serializable_data = _convert_spark_rows_to_json_serializable(collected_rows_as_dicts)
            Variable.set(data_variable_key, serializable_data, serialize_json=True)
        
        # --- 3. Update last_update_time on Success ---
        # The counter is already updated, so we just need to update the timestamp.
        print(f"Successfully executed '{view_name}'. Updating timestamp.")
        # We need to read the variable again in case another task modified it.
        # This is less critical but good practice.
        current_runtime_info = Variable.get(runtime_variable_key, deserialize_json=True)
        current_runtime_info[view_name]["last_update_time"] = pendulum.now().isoformat()
        Variable.set(runtime_variable_key, current_runtime_info, serialize_json=True)
        print("Timestamp updated successfully.")

    finally:
        # The finally block is now only for cleanup, like stopping Spark.
        if 'spark' in locals() and spark.getActiveSession():
            spark.stop()
            print("Spark session stopped.")

    return

# =============================================================================
# SECTION 3: DYNAMIC AIRFLOW DAG GENERATION
# =============================================================================

def setup_runtime_information(dag_id: str, views_in_pipeline: set):
    """
    Initializes all required Airflow Variables for both runtime state and data payloads.
    """
    runtime_variable_key = f"runtime_info_{dag_id}"
    print(f"Checking for runtime info variable: '{runtime_variable_key}'")
    
    try:
        current_info = Variable.get(runtime_variable_key, deserialize_json=True)
    except Exception: 
        current_info = None
    
    if current_info is None:
        print("Runtime info not found. Initializing new state.")
        initial_info = {}
        for view_name in views_in_pipeline:
            view_meta = DSL_METADATA["views"][view_name]
            lag_info = _parse_target_lag(view_meta["target_lag"])
            initial_info[view_name] = {"target_lag_seconds": lag_info['seconds'], "last_update_time": None, "counter": 0}
            if view_meta["materialize"]["type"] == "xcom":
                data_variable_key = f"data_payload_{dag_id}_{view_name}"
                try:
                    Variable.get(data_variable_key)
                except Exception:
                    print(f"Initializing data payload variable: '{data_variable_key}'")
                    Variable.set(data_variable_key, None, serialize_json=True)
        Variable.set(runtime_variable_key, initial_info, serialize_json=True)
        print("Successfully initialized all variables.")
    else:
        print("Runtime info already exists. No action needed.")

def create_dag_for_pipeline(sink_view_name, sink_view_meta):
    """
    Factory function to create a stateful, decoupled DAG for a sink view.
    """
    dag_id = f"pipeline_for_{sink_view_name}"
    
    views_in_pipeline = {sink_view_name}
    views_to_process = [sink_view_name]
    while views_to_process:
        current_view = views_to_process.pop(0)
        dependencies = DSL_METADATA["views"][current_view].get("depends_on", [])
        for dep in dependencies:
            if dep not in views_in_pipeline:
                views_in_pipeline.add(dep)
                views_to_process.append(dep)
    
    all_lags_seconds = [_parse_target_lag(DSL_METADATA["views"][v]["target_lag"])['seconds'] for v in views_in_pipeline]
    base_refresh_rate_seconds = _calculate_gcd_for_list(all_lags_seconds) or _parse_target_lag(sink_view_meta["target_lag"])['seconds']

    with DAG(
        dag_id=dag_id,
        max_active_runs=1,
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        schedule=timedelta(seconds=base_refresh_rate_seconds),
        catchup=False,
        tags=['compiler-generated', 'stateful-pipeline']
    ) as dag:
        
        setup_task = PythonOperator(
            task_id='setup_runtime_information',
            python_callable=setup_runtime_information,
            op_kwargs={'dag_id': dag.dag_id, 'views_in_pipeline': views_in_pipeline}
        )

        tasks = {}
        
        def create_task_and_dependencies(view_name):
            if view_name in tasks:
                return

            view_meta = DSL_METADATA["views"][view_name]
            
            for dep_name in view_meta.get("depends_on", []):
                create_task_and_dependencies(dep_name)

            task_trigger_rule = TriggerRule.ALL_DONE if view_meta.get("depends_on") else TriggerRule.ALL_SUCCESS

            tasks[view_name] = execute_view_logic_stateful.override(
                task_id=f"execute_{view_name}",
                trigger_rule=task_trigger_rule
            )(
                view_name=view_name,
                dag_id=dag.dag_id,
                base_refresh_rate_seconds=base_refresh_rate_seconds
            )
            
            for dep_name in view_meta.get("depends_on", []):
                tasks[dep_name] >> tasks[view_name]

        create_task_and_dependencies(sink_view_name)

        root_tasks = [task for view, task in tasks.items() if not DSL_METADATA["views"][view].get("depends_on")]
        if root_tasks:
            setup_task >> root_tasks

    return dag

for view_name, view_meta in DSL_METADATA["views"].items():
    if view_meta["materialize"]["type"] == "sink":
        globals()[f"dag_for_{view_name}"] = create_dag_for_pipeline(view_name, view_meta)
