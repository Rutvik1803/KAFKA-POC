from faker import Faker
from pizzaproducer import PizzaProvider

fake = Faker()
fake.add_provider(PizzaProvider)

for i in range(10):
    pizza_name = fake.pizza_name()
    customer_name = fake.name()
    address = fake.address()
    phone_number = fake.phone_number()
    print(f"Order {i+1}:")
    print(f"Pizza: {pizza_name}")
    print(f"Customer: {customer_name}")
    print(f"Address: {address}")
    print(f"Phone: {phone_number}")
