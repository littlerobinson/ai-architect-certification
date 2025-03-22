import logging
import time
from datetime import datetime, timedelta

import boto3
import pandas as pd
import paramiko
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.ec2 import (
    EC2CreateInstanceOperator,
    EC2TerminateInstanceOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import create_engine

# Airflow variables
S3_BUCKET_NAME = Variable.get("S3BucketName")
S3_PATH = Variable.get("S3Path")
KEY_PAIR_NAME = Variable.get("KEY_PAIR_NAME")
KEY_PATH = Variable.get("KEY_PATH")  # Path to your private key inside the container
AMI_ID = Variable.get("AMI_ID")
SECURITY_GROUP_ID = Variable.get("SECURITY_GROUP_ID")
INSTANCE_TYPE = Variable.get("INSTANCE_TYPE")
MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_ID = Variable.get("MLFLOW_EXPERIMENT_ID")
MLFLOW_LOGGED_MODEL = Variable.get("MLFLOW_LOGGED_MODEL")
SLACK_TOKEN = Variable.get("SLACK_TOKEN")

# Airflow connexions
aws_conn = BaseHook.get_connection("aws_default")
postgres_conn = BaseHook.get_connection("postgres_default")

# Get AWS connection details from Airflow
aws_access_key_id = aws_conn.login
aws_secret_access_key = aws_conn.password
region_name = aws_conn.extra_dejson.get("region_name", "eu-west-3")
AWS_ACCESS_KEY_ID = aws_access_key_id
AWS_SECRET_ACCESS_KEY = aws_secret_access_key

# DB_PASSWORD = Variable.get("DB_PASSWORD")
# DB_HOST = Variable.get("DB_HOST")
# DB_PORT = Variable.get("DB_PORT")
# DB_NAME = Variable.get("DB_NAME")
# DB_USER = Variable.get("DB_USER")

# Get Postgres connection details from Airflow
DB_USER = postgres_conn.login
DB_PASSWORD = postgres_conn.password
DB_HOST = postgres_conn.host
DB_PORT = postgres_conn.port
DB_NAME = postgres_conn.schema

# Email
EMAIL_NOTIFICATION = Variable.get("EMAIL_NOTIFICATION")

# Slack configuration
slack_token = SLACK_TOKEN
slack_channel = "#airflow"
slack_url = "https://slack.com/api/chat.postMessage"


def failure_slack_alert(context):
    slack_webhook = SlackWebhookHook(webhook_token=slack_token)
    message = f":x: Task {context['task_instance_key_str']} fail !"
    slack_webhook.send_text(message)


def _check_payments_table_data():
    logging.info("_check_payments_table_data")

    table_name = "payments"

    # Get PostgreSQL connection
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]

    # Close cursor and connection
    cursor.close()
    conn.close()

    if count == 0:
        # Log if the table is empty
        logging.info("The table %s is empty. Proceeding to load CSV.", table_name)
        return "initialization_branch.load_initial_payments"
    else:
        # Log if the table is not empty
        logging.info("The table %s is not empty. No loading performed.", table_name)
        return "initialization_branch.do_not_load_initial_payments"


def _do_not_load_initial_payments():
    logging.info("Table is not empty. No action taken.")


def _load_initial_payments():
    logging.info("_load_initial_payments function")
    table_name = "payments"
    csv_file_path = "/opt/airflow/data/fraudTest.zip"

    # Get PostgreSQL connection
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = postgres_hook.get_conn()

    # Load CSV into a DataFrame
    logging.info("Reading CSV source file for training.")
    df = pd.read_csv(csv_file_path)

    # Rename columns to match the database schema
    logging.info("Renaming columns to match the database schema.")
    df.rename(
        columns={
            "cc_num": "cc_num",
            "merchant": "merchant",
            "category": "category",
            "amt": "amount",
            "first": "first_name",
            "last": "last_name",
            "gender": "gender",
            "street": "street",
            "city": "city",
            "state": "state",
            "zip": "zip",
            "lat": "lat",
            "long": "long",
            "city_pop": "city_pop",
            "job": "job",
            "dob": "dob",
            "trans_num": "trans_num",
            "unix_time": "cur_time",
            "merch_lat": "merch_lat",
            "merch_long": "merch_long",
            "is_fraud": "is_fraud",
        },
        inplace=True,
    )

    # Drop unnecessary columns
    logging.info("Dropping unnecessary columns.")
    df.drop(columns=["Unnamed: 0", "trans_date_trans_time"], inplace=True)

    # Convert boolean columns
    logging.info("Converting boolean columns.")
    df["is_fraud"] = df["is_fraud"].astype(bool)

    # Create a SQLAlchemy engine to connect to PostgreSQL
    engine = create_engine(postgres_hook.get_uri())

    # Use to_sql to load the data
    logging.info(f"Inserting data into table {table_name} using to_sql.")
    try:
        # Insert data into the PostgreSQL table
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",  # Add data in table
            index=False,  # Do not insert the DataFrame index
            chunksize=10000,  # Split data into chunks for faster insertion
        )
        logging.info("Data successfully loaded into the table using to_sql.")
    except Exception as e:
        logging.error(f"Error during data insertion: {e}")
    finally:
        conn.close()


dag_default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 1,
    "on_failure_callback": failure_slack_alert,
}


with DAG(
    "training_for_frauds_detection",
    default_args=dag_default_args,
    description="Download a CSV file and load it into an S3 bucket",
    schedule_interval="0 0 * * 1",  # Every monday at midnight
    catchup=False,
    tags=["ml-training", "postgres", "ec2"],
) as dag:
    logging.info("download_and_upload_raw_data_to_s3")

    start = BashOperator(task_id="start", bash_command="echo 'Start!'")

    with TaskGroup(group_id="initialization_branch") as initialization_branch:
        logging.info("initialization_branch")

        check_payments_table_db = PostgresOperator(
            task_id="check_payments_table_db",
            postgres_conn_id="postgres_default",
            sql="""
                CREATE TABLE IF NOT EXISTS payments (
                    cc_num BIGINT,
                    merchant VARCHAR(255),
                    category VARCHAR(255),
                    amount DECIMAL(10, 2),
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    gender CHAR(1),
                    street VARCHAR(255),
                    city VARCHAR(255),
                    state VARCHAR(255),
                    zip VARCHAR(10),
                    lat DECIMAL(9, 6),
                    long DECIMAL(9, 6),
                    city_pop INTEGER,
                    job VARCHAR(255),
                    dob DATE,
                    trans_num VARCHAR(255) PRIMARY KEY,
                    merch_lat DECIMAL(9, 6),
                    merch_long DECIMAL(9, 6),
                    is_fraud BOOLEAN,
                    fraud_predict BOOLEAN,
                    cur_time BIGINT
                );
            """,
            dag=dag,
        )

        check_payment_predictions_table_db = PostgresOperator(
            task_id="check_payment_predictions_table_db",
            postgres_conn_id="postgres_default",
            sql="""
                CREATE TABLE IF NOT EXISTS payment_predictions (
                    cc_num BIGINT,
                    merchant VARCHAR(255),
                    category VARCHAR(255),
                    amount DECIMAL(10, 2),
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    gender CHAR(1),
                    street VARCHAR(255),
                    city VARCHAR(255),
                    state VARCHAR(255),
                    zip VARCHAR(10),
                    lat DECIMAL(9, 6),
                    long DECIMAL(9, 6),
                    city_pop INTEGER,
                    job VARCHAR(255),
                    dob DATE,
                    trans_num VARCHAR(255) PRIMARY KEY,
                    merch_lat DECIMAL(9, 6),
                    merch_long DECIMAL(9, 6),
                    is_fraud BOOLEAN,
                    fraud_predict BOOLEAN,
                    cur_time BIGINT
                );
            """,
            dag=dag,
        )

        check_payments_table_data = BranchPythonOperator(
            task_id="check_payments_table_data",
            python_callable=_check_payments_table_data,
            provide_context=True,
        )

        # Dummy operator to mark the end of branches
        end_of_initialization_branch = BashOperator(
            task_id="end_of_initialization_branch",
            bash_command="echo 'End of initialization branch, table payment created and initial data placed inside'",
            trigger_rule="one_success",  # Proceed if at least one upstream task succeeds
        )

        load_initial_payments = PythonOperator(
            task_id="load_initial_payments",
            python_callable=_load_initial_payments,
            execution_timeout=timedelta(minutes=30),
        )

        do_not_load_initial_payments = PythonOperator(
            task_id="do_not_load_initial_payments",
            python_callable=_do_not_load_initial_payments,
        )

        (
            check_payments_table_db
            >> check_payment_predictions_table_db
            >> check_payments_table_data
            >> [load_initial_payments, do_not_load_initial_payments]
            >> end_of_initialization_branch
        )

    with TaskGroup(group_id="training_branch") as training_branch:
        logging.info("training_branch")

        create_ec2_instance = EC2CreateInstanceOperator(
            task_id="create_ec2_instance",
            region_name=region_name,
            image_id=AMI_ID,
            max_count=1,
            min_count=1,
            config={  # Dictionary for arbitrary parameters to the boto3 `run_instances` call
                "InstanceType": INSTANCE_TYPE,
                "KeyName": KEY_PAIR_NAME,
                "SecurityGroupIds": [SECURITY_GROUP_ID],
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [{"Key": "Purpose", "Value": "ML-Training"}],
                    }
                ],
            },
            wait_for_completion=True,  # Wait for the instance to be running before proceeding
        )

        @task
        def check_ec2_status(instance_id):
            """Check if the EC2 instance has passed both status checks (2/2 checks passed)."""

            ec2_client = boto3.client(
                "ec2",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
            )
            passed_checks = False

            while not passed_checks:
                # Get the instance status
                response = ec2_client.describe_instance_status(InstanceIds=instance_id)

                # Check if there is any status information returned
                if response["InstanceStatuses"]:
                    instance_status = response["InstanceStatuses"][0]

                    system_status = instance_status["SystemStatus"]["Status"]
                    instance_status_check = instance_status["InstanceStatus"]["Status"]

                    # Log the current status
                    logging.info(
                        f"System Status: {system_status}, Instance Status: {instance_status_check}"
                    )

                    # Check if both status checks are passed
                    if system_status == "ok" and instance_status_check == "ok":
                        logging.info(
                            f"Instance {instance_id} has passed 2/2 status checks."
                        )
                        passed_checks = True
                    else:
                        logging.info(
                            f"Waiting for instance {instance_id} to pass 2/2 status checks..."
                        )
                else:
                    logging.info(
                        f"No status available for instance {instance_id} yet. Waiting..."
                    )

                # Wait before polling again
                time.sleep(15)

            return True

        @task
        def get_ec2_public_ip(instance_id):
            """Retrieve the EC2 instance public IP for SSH."""

            # Initialize the EC2 resource using boto3 with credentials from Airflow connection
            ec2 = boto3.resource(
                "ec2",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
            )

            # Access EC2 instance by ID
            instance = ec2.Instance(instance_id[0])

            # Wait for the instance to be running
            instance.wait_until_running()
            instance.reload()

            # Get the instance's public IP
            public_ip = instance.public_ip_address
            logging.info(f"Public IP of EC2 Instance: {public_ip}")

            # Return the public IP for the SSH task
            return public_ip

        ec2_public_ip = get_ec2_public_ip(create_ec2_instance.output)
        check_ec2_instance = check_ec2_status(create_ec2_instance.output)

        logging.info(f"ec2 instance output {create_ec2_instance.output}")

        @task
        def run_training_via_paramiko(public_ip):
            """Use Paramiko to SSH into the EC2 instance and run ML training."""

            print("PUBLIC IP:", public_ip)
            # Initialize SSH client
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy()
            )  # Automatically add unknown hosts

            # Load private key
            private_key = paramiko.RSAKey.from_private_key_file(KEY_PATH)

            try:
                # Establish an SSH connection
                ssh_client.connect(
                    hostname=public_ip, username="ubuntu", pkey=private_key
                )

                # Export environment variables
                # command = f"""
                # export MLFLOW_TRACKING_URI={MLFLOW_TRACKING_URI}
                # export MLFLOW_EXPERIMENT_ID={MLFLOW_EXPERIMENT_ID}
                # export AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}
                # export AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}
                # export PATH=$PATH:/home/ubuntu/.local/bin
                # mlflow run https://github.com/littlerobinson/ai-architect-certification.git/automatic_fraud_detection/training --build-image
                # """

                command = f"""
                export MLFLOW_TRACKING_URI={MLFLOW_TRACKING_URI}
                export MLFLOW_EXPERIMENT_ID={MLFLOW_EXPERIMENT_ID}
                export MLFLOW_LOGGED_MODEL={MLFLOW_LOGGED_MODEL}
                export MLFLOW_EXPERIMENT_NAME="automatic-fraud-detection"
                export AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}
                export AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}
                export DB_HOST={DB_HOST}
                export DB_USER={DB_USER}
                export DB_PASSWORD={DB_PASSWORD}
                export DB_PORT={DB_PORT}
                export DB_NAME={DB_NAME}
                git clone https://github.com/littlerobinson/ai-architect-certification.git
                cd ai-architect-certification/automatic_fraud_detection/training
                docker build -t automatic-fraud-detection-training .
                cp run.sh.example run.sh
                chmod +x run.sh
                ./run.sh
                """

                # Run your training command via SSH
                stdin, stdout, stderr = ssh_client.exec_command(command)

                # stdout_text = ""
                # stderr_text = ""

                # while not stdout.channel.exit_status_ready():
                #     if stdout.channel.recv_ready():
                #         stdout_text += stdout.channel.recv(1024).decode()
                #     if stdout.channel.recv_stderr_ready():
                #         stderr_text += stdout.channel.recv_stderr(1024).decode(
                #             "utf-8", errors="ignore"
                #         )

                #     time.sleep(5)  # Wait before verify a new time

                # # Read outputs
                # stdout_text += stdout.read().decode()
                # stderr_text += stderr.read().decode()

                # # Display outputs
                # logging.info(stdout_text)
                # logging.error(stderr_text)

                # Wait for the command to complete
                exit_status = stdout.channel.recv_exit_status()

                # Read the outputs
                stdout_text = stdout.read().decode()
                stderr_text = stderr.read().decode()

                # Log the outputs
                logging.info(f"STDOUT:\n{stdout_text}")
                logging.error(f"STDERR:\n{stderr_text}")

                if exit_status != 0:
                    logging.error(f"Command failed with exit status: {exit_status}")
                else:
                    logging.info("Command executed successfully.")

                # logging.info("Command executed successfully.")

            except Exception as e:
                logging.error(f"Error occurred during SSH: {str(e)}")
                raise
            finally:
                # Close the SSH connection
                logging.info("Close the SSH connection")
                ssh_client.close()

        run_training = run_training_via_paramiko(ec2_public_ip)

        terminate_instance = EC2TerminateInstanceOperator(
            task_id="terminate_ec2_instance",
            region_name=region_name,
            instance_ids=create_ec2_instance.output,
            wait_for_completion=True,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        (
            create_ec2_instance
            >> ec2_public_ip
            >> check_ec2_instance
            >> run_training
            >> terminate_instance
        )

    end = BashOperator(task_id="end", bash_command="echo 'End!'")

    start >> initialization_branch >> training_branch >> end
