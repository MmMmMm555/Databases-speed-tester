import psycopg2
from datetime import datetime, timedelta
from faker import Faker
from clickhouse_driver import Client
import random

clickhouse = Client(host='localhost', port=9000, user='default', password='', database='default')

postgres = psycopg2.connect(host='localhost', port=5432, user='postgres', password='admin', database='postgres_speed_test')
cursor = postgres.cursor()

fake = Faker()

time_for_clickhouse = timedelta(seconds=0)
time_for_postgres = timedelta(seconds=0)

all_clickhouse_data = clickhouse.execute("SELECT * FROM product order by id limit 1000")
cursor.execute("SELECT id, price FROM product order by id limit 1000;")
all_postgres_data = cursor.fetchall()

for data in all_clickhouse_data:
    price = random.randint(18, 657666)
    from_time = datetime.now()
    clickhouse.execute(f"ALTER TABLE product UPDATE price = {price} WHERE id = {data[0]}")
    to_time = datetime.now()
    time_for_clickhouse += to_time - from_time

print(f"for single update. Total time for ClickHouse: {time_for_clickhouse}")

for data in all_postgres_data:
    price = random.randint(18, 657666)
    from_time = datetime.now()
    query = f"UPDATE product SET price = {price} WHERE id = {data[0]}"
    cursor.execute(query)
    postgres.commit()
    to_time = datetime.now()
    time_for_postgres += to_time - from_time

print(f"for single update. Total time for Postgres: {time_for_postgres}")

clickhouse.disconnect()
cursor.close()
postgres.close()
