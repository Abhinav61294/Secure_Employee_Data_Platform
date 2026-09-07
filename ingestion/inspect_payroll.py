import psycopg
from dotenv import load_dotenv
import os

load_dotenv()

connection = psycopg.connect(
    dbname="payroll_db",
    user="postgres",
    password=os.getenv("POSTGRES_PASSWORD"),
    host="localhost",
    port=5432,
)

with connection.cursor() as cursor:
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'payroll'
        ORDER BY ordinal_position;
    """)

    for row in cursor.fetchall():
        print(row)

connection.close()