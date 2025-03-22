import logging
import os

import mlflow
import pandas as pd
from dotenv import load_dotenv
from sklearn.compose import ColumnTransformer
from sklearn.discriminant_analysis import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import accuracy_score, f1_score
from mlflow.models import infer_signature
import pandas as pd
from sqlalchemy import create_engine


logging.basicConfig(level=logging.INFO)


def load_data_from_db():
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    engine = create_engine(
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require"
    )

    logging.warning("postgres uri")
    logging.warning(
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require"
    )

    query = "SELECT * FROM payments WHERE is_fraud IS NOT NULL"

    fraud_df = pd.read_sql(query, con=engine)

    print("Data successfully loaded from the database.")
    return fraud_df


if __name__ == "__main__":
    logging.info("automatic fraud detection start app")

    logging.info("init mlflow credentials")
    # load the environment variables
    load_dotenv()

    # Init for MLflow
    MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI")
    logging.info(f"call mlflow uri: {MLFLOW_TRACKING_URI}")
    MLFLOW_LOGGED_MODEL = os.getenv("MLFLOW_LOGGED_MODEL")
    logging.info(f"call mlflow logged path for artifact: {MLFLOW_LOGGED_MODEL}")

    EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME")
    # mlflow.set_experiment(EXPERIMENT_NAME)
    # experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    logging.info(f"experiment name {EXPERIMENT_NAME}")
    mlflow.sklearn.autolog()

    with mlflow.start_run():
        logging.info("Load data for training")
        fraud_df = load_data_from_db()

        logging.warning(fraud_df.head(2))

        logging.info("Preparing data")
        target_column = "is_fraud"

        # Specify columns for trainings
        useful_columns = [
            "is_fraud",  # La colonne cible
            "amount",
            "category",
            "merchant",
            "gender",
            "city",
            "state",
            "job",
        ]

        # Filtrer le DataFrame pour ne garder que les colonnes utiles
        fraud_df = fraud_df[useful_columns]

        X = fraud_df.drop(columns=[target_column])  # Features
        y = fraud_df[target_column]  # Target

        numerical_columns = X.select_dtypes(include=["number"]).columns
        categorical_columns = X.select_dtypes(exclude=["number"]).columns

        preprocessor = ColumnTransformer(
            [
                ("num", StandardScaler(), numerical_columns),
                ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_columns),
            ]
        )

        pipeline = Pipeline(
            [
                ("preprocessing", preprocessor),
                (
                    "classifier",
                    RandomForestClassifier(n_estimators=20, random_state=42, n_jobs=-1),
                ),
            ]
        )

        # Update int columns to float 64
        X = X.astype({col: "float64" for col in X.select_dtypes(include="int").columns})

        logging.info("Splitting data")
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        logging.info("Training model")
        pipeline.fit(X_train, y_train)

        logging.info("Evaluating model")
        y_pred = pipeline.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)

        logging.info(f"Accuracy: {acc:.4f}, F1 Score: {f1:.4f}")
        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("f1_score", f1)

        # Define input example and model signature
        # input_example = X_train.iloc[:1].to_dict(orient="list")
        input_example = X_train.sample(5, random_state=42).to_dict(orient="list")

        signature = infer_signature(X_train, pipeline.predict(X_train))

        mlflow.sklearn.log_model(
            pipeline,
            "fraud_detection_model",
            signature=signature,
            input_example=input_example,
        )

        logging.info("MLflow run completed")

        logging.info("MLflow run completed")
