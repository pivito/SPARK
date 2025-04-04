import psycopg2
from psycopg2 import Error

def insert_data(data):
    connection=None
    try:
        # Establishing connection to PostgreSQL database
        connection = psycopg2.connect(
            user="postgres",
            password="casa1234",
            host="localhost",
            port="5432",
            database="retail_db"
        )

        # Creating a cursor object using the cursor() method
        cursor = connection.cursor()
        
        create_table_query = '''CREATE TABLE IF NOT EXISTS ExampleData (
                                column1 VARCHAR(255),
                                column2 VARCHAR(255),
                                column3 VARCHAR(255)
                            );'''

        # Executing the SQL command to create the table
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully")

        # Constructing the PostgreSQL INSERT statement
        insert_query = """ INSERT INTO ExampleData (column1, column2, column3)
                           VALUES (%s, %s, %s)"""

        # Executing the SQL command with the provided data
        cursor.execute(insert_query, data)

        # Commit your changes to the database
        connection.commit()

        print("Data inserted successfully")

    except (Exception, Error) as error:
        print("Error while inserting data into PostgreSQL:", error)

    finally:
        # Closing database connection
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

# Example data to be inserted into the database
data_to_insert = ('value1', 'value2', 'value3')

# Call the insert_data function with the example data
insert_data(data_to_insert)
