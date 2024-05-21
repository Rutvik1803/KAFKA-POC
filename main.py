import json 
from kafka import KafkaProducer
from faker import Faker
from pizzaproducer import PizzaProvider


TOPIC_NAME = "Pizza-Orders-Topic"
folderName = "./"
producer = KafkaProducer(
    bootstrap_servers=f"",
    security_protocol="SSL",
    ssl_cafile=folderName+"ca.pem",
    ssl_certfile=folderName+"service.cert",
    ssl_keyfile=folderName+"service.key",
    value_serializer=lambda v: json.dumps(v).encode('ascii'),
    key_serializer=lambda v: json.dumps(v).encode('ascii'),
)

# Function to generate pizza order
def generate_order(fake):
    return {
        'pizza_name': fake.pizza_name(),
        'customer_name': fake.name(),
        'address': fake.address(),
        'phone_number': fake.phone_number()
    }

def main():
    fake = Faker()
    fake.add_provider(PizzaProvider)

    for i in range(25):
        order = generate_order(fake)
        producer.send(TOPIC_NAME, key=str(i), value=order)
        print(f"Sent order {i+1}: {order}")

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
