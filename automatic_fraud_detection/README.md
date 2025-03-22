# Automatic Fraud Detection

## Infos

Payement API documentation

## Installation

### Traing model :

```bash
cd mlflow docker build -t automatic-fraud-detection-mlflow .
cd ..
./run.sh
```

### Consume payments data :

- Create the database on your postgresdb automatic-fraud-detection.

- Launch the container docker for kafka service.

```bash
cd kafka
docker compose up -d
# 2 container will appear, if just one appear perhaps it's an old volume the problem, do :
docker compose down -v
```

- Launch kafka producer & consumer :

```bash
cd kafka
python app/producer.py
python app/consumer.py
```

### EC2

Create an EC2 instance with all packages for training and save it as instant.
Create a key pair to connect to EC2 and save it in the secret folder inside Airflow.

### Airflow

```bash
cd airflow
docker compose up airflow-init
docker compose up -d
```

Import variables from json file in secrets folder.

Create connections :

```
aws_default
postgres_default
slack_default
```

```markdown
Connection Id: postgres_default
Connection Type: Postgres
Host: ep-small-xxx
Database: automatic-fraud-detection
Login: xxx
Password: xxx
Port: 5432
Extra: {"sslmode": "require"}
```

### Slack

Manage access via : https://api.slack.com/apps/

Create APP, access token and permissions.

![alt text](slack1.png)

Help --> https://help.thebotplatform.com/en/articles/7233667-how-to-create-a-slack-bot
