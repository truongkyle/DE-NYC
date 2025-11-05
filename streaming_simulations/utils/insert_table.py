import os
from dotenv import load_dotenv
import random
from datetime import datetime, timedelta
from time import sleep
from postgresql_client import PostgresSQLClient

load_dotenv()

TABLE_NAME = "rides"
NUM_ROWS = 1000  # thay đổi nếu bạn muốn chèn nhiều hơn

def random_pickup_and_dropoff(max_days_back=30):
    now = datetime.now()
    delta_days = random.uniform(0, max_days_back)
    pickup = now - timedelta(days=delta_days, seconds=random.randint(0, 86400))
    duration_minutes = random.randint(1, 120)
    dropoff = pickup + timedelta(minutes=duration_minutes)
    return pickup, dropoff

def random_money(min_val=5.0, max_val=100.0, precision=2):
    v = round(random.uniform(min_val, max_val), precision)
    return v

def main():
    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    # optional: kiểm tra schema / cột
    try:
        columns = pc.get_colums(table_name=TABLE_NAME)
        print(f"Columns for table {TABLE_NAME}: {columns}")
    except Exception as e:
        print(f"Failed to get schema for table with error: {e}")
        # không dừng, tiếp tục cố gắng chèn dữ liệu

    inserted = 0
    for _ in range(NUM_ROWS):
        try:
            pickup_ts, dropoff_ts = random_pickup_and_dropoff(max_days_back=30)
            fare = random_money(5.0, 120.0)
            tip = random_money(0.0, min(0.3 * fare, 50.0))  # tip không quá 30% hoặc 50$
            total = round(fare + tip, 2)

            # format timestamp ISO cho Postgres
            pickup_str = pickup_ts.strftime("%Y-%m-%d %H:%M:%S")
            dropoff_str = dropoff_ts.strftime("%Y-%m-%d %H:%M:%S")

            query = f"""
            INSERT INTO {TABLE_NAME} (pickup_ts, dropoff_ts, fare, tip, total)
            VALUES ('{pickup_str}', '{dropoff_str}', {fare}, {tip}, {total});
            """

            pc.execute_query(query)
            sleep(3)
            inserted += 1
            print(f"[{inserted}/{NUM_ROWS}] Inserted ride: pickup={pickup_str}, dropoff={dropoff_str}, fare={fare}, tip={tip}, total={total}")

            # nếu muốn throttle tốc độ chèn (dev/test), chỉnh sleep ở đây
            # sleep(0.05)  # mặc định để rất nhỏ để nhanh
        except Exception as e:
            print(f"Failed to insert row: {e}")
            # tiếp tục vòng lặp để thử chèn các hàng tiếp theo

    print(f"Done. Inserted {inserted} rows into {TABLE_NAME}.")

if __name__ == "__main__":
    main()
