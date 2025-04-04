
import psycopg2
import random
import string

def get_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def create_table():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="retail_db",
            user="postgres",
            password="casa1234",
            port=5432
        )
        createTableString='CREATE TABLE IF NOT EXISTS Stores ( store_id SERIAL PRIMARY KEY, store_name VARCHAR(255) NOT NULL, location VARCHAR(255) NOT NULL, demographics VARCHAR(255) NOT NULL);'
        with conn.cursor() as cur:
            cur.execute(createTableString)
        conn.commit()
        

        with conn.cursor() as cur:
            for x in range(1000):
                insertString=f'INSERT INTO Stores  (store_name,location,demographics) VALUES (\'{get_random_string(30)}\',\'{get_random_string(20)}\',\'{get_random_string(5)}\')'
                cur.execute(insertString)
            conn.commit()

                
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    create_table()