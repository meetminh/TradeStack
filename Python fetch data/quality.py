import psycopg2
from psycopg2 import OperationalError

try:
    print("Attempting to connect...")
    # Using the exact same domain that works for your web interface
    host = "questdb.go-server-devcontainer.orb.local"
    conn = psycopg2.connect(
        host=host,
        port=8812,
        user="admin",
        password="quest",
        dbname="qdb",
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
