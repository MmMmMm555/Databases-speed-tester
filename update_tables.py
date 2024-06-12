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

price = random.randint(18, 657666)

# query1 = f"UPDATE product SET price = 98897898;"
query1 = f"UPDATE product SET price = 1111111111 WHERE id != 1;"

from_time = datetime.now()
cursor.execute(query1)
postgres.commit()
to_time = datetime.now()
time_for_postgres = to_time - from_time

start_time = datetime.now()
clickhouse.execute(query1)
end_time = datetime.now()
time_for_clickhouse = end_time - start_time

print(f"For query: '{query1}' Total time: {time_for_clickhouse} in ClickHouse")
print(f"For query: '{query1}' Total time: {time_for_postgres} in Postgres")

clickhouse.disconnect()
cursor.close()
postgres.close()
