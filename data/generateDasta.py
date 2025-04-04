import csv
import random
from datetime import datetime, timedelta
from decimal import Decimal

def generate_random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

def generate_random_string(length=8):
    letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    return ''.join(random.choice(letters) for _ in range(length))

def generate_random_decimal():
    return Decimal(random.uniform(1, 1000))

# Define column names
columns = ["Date", "Product", "Category", "Price", "Quantity", "Total", "Discount", "Customer", "Payment Method", "Status"]

# Generate random data for each row
data = []
start_date = datetime(2020, 1, 1)
end_date = datetime(2025, 12, 31)

for _ in range(1000):
    row = [
        generate_random_date(start_date, end_date).strftime('%Y-%m-%d'),
        generate_random_string(),
        generate_random_string(),
        round(random.uniform(1, 1000), 2),
        random.randint(1, 100),
        round(random.uniform(1, 500), 2),
        round(random.uniform(0, 50), 2),
        generate_random_string(),
        random.choice(["Cash", "Credit Card", "Debit Card"]),
        random.choice(["Pending", "Completed", "Cancelled"])
    ]
    data.append(row)

# Write data to CSV file
with open('shop_data.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(columns)
    writer.writerows(data)

print("CSV file generated successfully.")
