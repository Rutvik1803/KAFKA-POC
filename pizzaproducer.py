import random
from faker.providers import BaseProvider  # Corrected import statement

class PizzaProvider(BaseProvider):
    def pizza_name(self):
        validPizzaNames = ['Margherita', 'Marina', 'Diavola', 'Salami', 'Pepperoni']  # Corrected spelling
        return random.choice(validPizzaNames)  # Simplified the random selection
