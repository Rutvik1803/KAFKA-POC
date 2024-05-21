import json
import psycopg2
from kafka import KafkaConsumer
import logging

# Kafka configuration
TOPIC_NAME = "Pizza-Orders-Topic"
KAFKA_BOOTSTRAP_SERVERS = ""
FOLDER_NAME = "./"
GROUP_ID = "pizza-orders-group"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    security_protocol="SSL",
    ssl_cafile=FOLDER_NAME + "ca.pem",
    ssl_certfile=FOLDER_NAME + "service.cert",
    ssl_keyfile=FOLDER_NAME + "service.key",
    value_deserializer=lambda v: json.loads(v.decode('ascii')),
    key_deserializer=lambda v: json.loads(v.decode('ascii')),
    group_id=GROUP_ID,  # Add group_id here
    auto_offset_reset='earliest',
    enable_auto_commit=False
)


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def store_order_in_db(order):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO orders (pizza_name, customer_name, address, phone_number)
            VALUES (%s, %s, %s, %s)
            """,
            (order['pizza_name'], order['customer_name'], order['address'], order['phone_number'])
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Failed to store order in DB: {e}")
        return False

def main():
    for message in consumer:
        order = message.value
        logger.info(f"Received order: {order}")

        if store_order_in_db(order):
            consumer.commit()
            logger.info(f"Stored order in DB and committed offset: {message.offset}")
            consumer.poll(timeout_ms=0)
        else:
            logger.error("Failed to store order in DB, not committing offset.")

if __name__ == "__main__":
    main()
