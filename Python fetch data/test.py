import psycopg2

host = "host.docker.internal"
port = "8812"
user = "admin"
password = "quest"
database = "qdb"

try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )

    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()[0]
    print(f"Connected successfully. QuestDB version: {version}")

    cursor.close()
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")
