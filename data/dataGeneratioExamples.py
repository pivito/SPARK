#pip install fastparquet
import os
import random
import pandas as pd
import json
from faker import Faker
from pathlib import Path

source_path = Path(__file__).resolve()
output_dir = source_path.parent
os.makedirs(output_dir, exist_ok=True)

# Initialize the Faker instance
fake = Faker()



# Function to generate random data for each problem

def generate_text_file(file_name, num_lines=100):
    with open(f"{output_dir}/{file_name}", 'w') as f:
        for _ in range(num_lines):
            f.write(f"{fake.sentence()}\n")

def generate_csv_file(file_name, num_rows=100):
    data = []
    for _ in range(num_rows):
        name = fake.name()
        address = fake.address()
        age = random.randint(18, 80)
        salary = random.randint(20000, 120000)
        data.append([name, address, age, salary])
    df = pd.DataFrame(data, columns=['Name', 'Address', 'Age', 'Salary'])
    df.to_csv(f"{output_dir}/{file_name}", index=False)

def generate_json_file(file_name, num_rows=100):
    data = []
    for _ in range(num_rows):
        product_id = fake.uuid4()
        category = fake.word()
        price = round(random.uniform(5.0, 1000.0), 2)
        data.append({"product_id": product_id, "category": category, "price": price})
    with open(f"{output_dir}/{file_name}", 'w') as f:
        json.dump(data, f, indent=4)

def generate_parquet_file(file_name, num_rows=100):
    data = []
    for _ in range(num_rows):
        product_id = fake.uuid4()
        category = fake.word()
        price = round(random.uniform(5.0, 1000.0), 2)
        data.append({"product_id": product_id, "category": category, "price": price})
    df = pd.DataFrame(data)
    df.to_parquet(f"{output_dir}/{file_name}", index=False)

def generate_order_data(file_name, num_rows=100):
    data = []
    for _ in range(num_rows):
        customer_id = fake.uuid4()
        product_id = fake.uuid4()
        quantity = random.randint(1, 5)
        total_price = round(random.uniform(10.0, 500.0), 2)
        data.append([customer_id, product_id, quantity, total_price])
    df = pd.DataFrame(data, columns=['customer_id', 'product_id', 'quantity', 'total_price'])
    df.to_csv(f"{output_dir}/{file_name}", index=False)
    print(f"{output_dir}/{file_name}")

# Generate required files
generate_text_file("text_data.txt", 200)
generate_csv_file("employees.csv", 200)
generate_csv_file("users.csv", 150)
generate_csv_file("products.csv", 100)
generate_csv_file("orders.csv", 150)
generate_json_file("sales_data.json", 150)
generate_json_file("product_data.json", 100)
generate_parquet_file("filtered_products.parquet", 100)
generate_parquet_file("products_parquet.parquet", 100)
generate_order_data("purchases.csv", 150)
