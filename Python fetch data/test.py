import psycopg2
from psycopg2 import OperationalError

# Using the exact domain from your screenshot
# Note the dash instead of dot after questdb
host = "host.docker.internal"
port = "8812"
user = "admin"
password = "quest"
database = "qdb"

try:
    print("Attempting to connect...")
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        connect_timeout=10
    )

    print("Connection established")
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()[0]
    print(f"Connected successfully. QuestDB version: {version}")

    cursor.close()
    conn.close()
except OperationalError as e:
    print(f"Connection timed out or failed: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
