from faker import Faker
import pandas as pd
import psycopg2
import random

fake = Faker()

def generate_store_data(n=50):
    data = []
    for _ in range(n):
        store_id = random.choice([random.randint(1, 100000), None, '', 'bad_id'])
        if isinstance(store_id, int):
            store_id_clean = store_id
        else:
            store_id_clean = None  # Forzar nulo si es inválido

        row = {
            'store_id': store_id_clean,
            'store_name': fake.company(),
            'location': random.choice([fake.city(), '', None]),
            'demographics': random.choice(['young', 'middle-aged', 'senior', None])
        }
        data.append(row)
    return pd.DataFrame(data)

def insert_into_postgres(df):
    conn = psycopg2.connect(
        dbname="retail_db",
        user="postgres",
        password="casa1234",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS Stores;")
    cur.execute("""
        CREATE TABLE Stores (
            store_id BIGINT,
            store_name TEXT,
            location TEXT,
            demographics TEXT
        );
    """)

    for _, row in df.iterrows():
        store_id = row['store_id']
        if pd.isna(store_id):  # Si es NaN, salta la fila
            print(f"Saltando fila inválida (store_id NaN): {row}")
            continue
        try:
            cur.execute("""
                INSERT INTO Stores (store_id, store_name, location, demographics)
                VALUES (%s, %s, %s, %s);
            """, (int(store_id), row['store_name'], row['location'], row['demographics']))
            conn.commit()
        except Exception as e:
            print(f" Error insertando fila: {row}, error: {e}")
            conn.rollback()

    cur.close()
    conn.close()

if __name__ == '__main__':
    df = generate_store_data()
    insert_into_postgres(df)
