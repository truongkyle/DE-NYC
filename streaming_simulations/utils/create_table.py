import os

from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

load_dotenv()

def main():
    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    # Create devices table
    create_table_query = """
        CREATE TABLE IF NOT EXISTS rides (
            ride_id SERIAL PRIMARY KEY,
            pickup_ts TIMESTAMP,
            dropoff_ts TIMESTAMP,
            fare DOUBLE PRECISION,
            tip DOUBLE PRECISION,
            total DOUBLE PRECISION
        );
    """
    try:
        pc.execute_query(create_table_query)
    except Exception as e:
        print(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()
