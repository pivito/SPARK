
from faker import Faker
import pandas as pd
import random
import os

fake = Faker()
output_path = '../apps/data_bda/csv/sales_data.csv'

def generate_sales_data(n=1000):
    data = []
    for _ in range(n):
        row = {
            'Date': fake.date(),
            'Store ID': random.choice([fake.random_int(min=1, max=100), '', None]),
            'Product ID': random.choice([fake.bothify(text='??-####'), None, 'BADID']),
            'Quantity Sold': random.choice([fake.random_int(min=1, max=50), '', 'NaN']),
            'Revenue': random.choice([round(random.uniform(5.0, 500.0), 2), '', None])
        }
        data.append(row)

    df = pd.DataFrame(data)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)

if __name__ == '__main__':
    generate_sales_data()
