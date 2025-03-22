import os
import mlflow
import pandas as pd
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

# Airflow variables
MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_ID = Variable.get("MLFLOW_EXPERIMENT_ID")
MLFLOW_LOGGED_MODEL = Variable.get("MLFLOW_LOGGED_MODEL")

# Airflow connections
aws_conn = BaseHook.get_connection("aws_default")
postgres_conn = BaseHook.get_connection("postgres_default")

# Get AWS credentials
AWS_ACCESS_KEY_ID = aws_conn.login
AWS_SECRET_ACCESS_KEY = aws_conn.password
region_name = aws_conn.extra_dejson.get("region_name", "eu-west-3")

# Define default arguments for the DAG
default_args = {"owner": "airflow", "start_date": days_ago(1)}

# Define the DAG
dag = DAG(
    "mlflow_model_prediction_dag",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["predict"],
)


# Function to load data from the database
def load_data_from_db(ti):
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = postgres_hook.get_conn()
    query = "SELECT * FROM payment_predictions WHERE fraud_predict IS NULL"
    df = pd.read_sql(query, conn)
    conn.close()
    ti.xcom_push(key="data", value=df.to_json())  # Stocker sous format JSON


# Function to make predictions and update the database
def make_and_update_predictions(ti):
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    # Load model
    model_uri = "runs:/5cf128eca2714645be3cbafe45d66d69/fraud_detection_model"
    model = mlflow.pyfunc.load_model(model_uri)

    # Load data from XCom
    data_json = ti.xcom_pull(task_ids="load_data", key="data")
    df = pd.read_json(data_json)

    # Convert columns to appropriate types
    df["amount"] = df["amount"].astype("float64")  # Convert amount to float64
    df["merchant"] = df["merchant"].astype("str")
    df["category"] = df["category"].astype("str")
    df["gender"] = df["gender"].astype("str")
    df["city"] = df["city"].astype("str")
    df["state"] = df["state"].astype("str")
    df["job"] = df["job"].astype("str")

    df["fraud_predict"] = df["fraud_predict"].fillna(0)  # Example: replace NaN with 0

    if "is_fraud_from_api" in df.columns:
        df["is_fraud_from_api"] = df["is_fraud_from_api"].fillna(False)
    else:
        df["is_fraud_from_api"] = False

    # Make predictions
    df["predict"] = model.predict(df)

    # Update the database
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = postgres_hook.get_conn()
    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            update_query = """
                UPDATE payment_predictions
                SET fraud_predict = %s
                WHERE cc_num = %s
            """
            cursor.execute(update_query, (row["predict"], row["cc_num"]))
    conn.commit()
    conn.close()


# Task to load the data
load_data_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data_from_db,
    provide_context=True,
    dag=dag,
)

# Task to make predictions and update the database
predict_and_update_task = PythonOperator(
    task_id="make_and_update_predictions",
    python_callable=make_and_update_predictions,
    provide_context=True,
    dag=dag,
)

# Define the task order
load_data_task >> predict_and_update_task
