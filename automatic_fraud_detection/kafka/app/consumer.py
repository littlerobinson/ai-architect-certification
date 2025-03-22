import os
import json
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()

    DB_CONFIG = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "sslmode": "require",  # neondb requirements
    }

    TOPIC_NAME = "real-time-payments"
    KAFKA_BROKER = "localhost:9092"

    # Configure Kafka Consumer
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for message in consumer:
        try:
            # Log the raw message value
            raw_value = message.value
            print(f"Raw message value: {raw_value}")

            data = json.loads(raw_value)
            # Ensure the message value is a dictionary
            if isinstance(data, dict) is False:
                print(type(data))
                raise ValueError("Message value is not a dictionary")

            # Extract necessary data
            columns = data.get("columns", [])
            data_values = data.get("data", [[]])[
                0
            ]  # Assuming there is always a single record

            # Create a dictionary from columns and values
            record = dict(zip(columns, data_values))

            # Extract specific values
            cc_num = record.get("cc_num")
            merchant = record.get("merchant")
            category = record.get("category")
            amount = record.get("amt")
            first_name = record.get("first")
            last_name = record.get("last")
            gender = record.get("gender")
            street = record.get("street")
            city = record.get("city")
            state = record.get("state")
            zip_code = record.get("zip")
            lat = record.get("lat")
            long = record.get("long")
            city_pop = record.get("city_pop")
            job = record.get("job")
            dob = record.get("dob")
            trans_num = record.get("trans_num")
            merch_lat = record.get("merch_lat")
            merch_long = record.get("merch_long")
            is_fraud = bool(record.get("is_fraud"))
            cur_time = record.get("current_time")

            # Insert data into the database
            cursor.execute(
                """
                INSERT INTO payment_predictions (
                    cc_num, merchant, category, amount, first_name, last_name, gender,
                    street, city, state, zip, lat, long, city_pop, job, dob,
                    trans_num, merch_lat, merch_long, "cur_time"
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                """,
                (
                    cc_num,
                    merchant,
                    category,
                    amount,
                    first_name,
                    last_name,
                    gender,
                    street,
                    city,
                    state,
                    zip_code,
                    lat,
                    long,
                    city_pop,
                    job,
                    dob,
                    trans_num,
                    merch_lat,
                    merch_long,
                    cur_time,
                ),
            )
            conn.commit()
            print(f"Data inserted: {record}")

        except Exception as e:
            conn.rollback()
            print(f"An error occurred: {e}")
            import traceback

            print(traceback.format_exc())

    cursor.close()
    conn.close()
