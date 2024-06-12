import psycopg2
from datetime import datetime, timedelta
from faker import Faker
from clickhouse_driver import Client
import random

clickhouse = Client(host='localhost', port=9000, user='default', password='', database='default')

postgres = psycopg2.connect(host='localhost', port=5432, user='postgres', password='admin', database='postgres_speed_test')
cursor = postgres.cursor()

fake = Faker()

product_names = [
    "Alpha Widget", "Beta Widget", "Gamma Widget", "Delta Widget", "Epsilon Widget",
    "Zeta Widget", "Eta Widget", "Theta Widget", "Iota Widget", "Kappa Widget",
    "Lambda Widget", "Mu Widget", "Nu Widget", "Xi Widget", "Omicron Widget",
    "Pi Widget", "Rho Widget", "Sigma Widget", "Tau Widget", "Upsilon Widget",
    "Phi Widget", "Chi Widget", "Psi Widget", "Omega Widget", "Alpha Gadget",
    "Beta Gadget", "Gamma Gadget", "Delta Gadget", "Epsilon Gadget", "Zeta Gadget",
    "Eta Gadget", "Theta Gadget", "Iota Gadget", "Kappa Gadget", "Lambda Gadget",
    "Mu Gadget", "Nu Gadget", "Xi Gadget", "Omicron Gadget", "Pi Gadget",
    "Rho Gadget", "Sigma Gadget", "Tau Gadget", "Upsilon Gadget", "Phi Gadget",
    "Chi Gadget", "Psi Gadget", "Omega Gadget", "Alpha Tool", "Beta Tool",
    "Gamma Tool", "Delta Tool", "Epsilon Tool", "Zeta Tool", "Eta Tool",
    "Theta Tool", "Iota Tool", "Kappa Tool", "Lambda Tool", "Mu Tool",
    "Nu Tool", "Xi Tool", "Omicron Tool", "Pi Tool", "Rho Tool", "Sigma Tool",
    "Tau Tool", "Upsilon Tool", "Phi Tool", "Chi Tool", "Psi Tool", "Omega Tool",
    "Alpha Device", "Beta Device", "Gamma Device", "Delta Device", "Epsilon Device",
    "Zeta Device", "Eta Device", "Theta Device", "Iota Device", "Kappa Device",
    "Lambda Device", "Mu Device", "Nu Device", "Xi Device", "Omicron Device",
    "Pi Device", "Rho Device", "Sigma Device", "Tau Device", "Upsilon Device",
    "Phi Device", "Chi Device", "Psi Device", "Omega Device", "Alpha Apparatus",
    "Beta Apparatus", "Gamma Apparatus", "Delta Apparatus", "Epsilon Apparatus",
    "Zeta Apparatus", "Eta Apparatus", "Theta Apparatus",]

time_for_clickhouse = timedelta(seconds=0)
time_for_postgres = timedelta(seconds=0)

query_count = 10000

for _ in range(query_count):

    name = random.choice(product_names)
    price = random.randint(18, 657666)

    query = f"INSERT INTO product (name, price) VALUES ('{name}' , {price});"

    from_time = datetime.now()
    cursor.execute(query)
    postgres.commit()
    to_time = datetime.now()
    time_for_postgres += to_time - from_time

    start_time = datetime.now()
    clickhouse.execute(query)
    end_time = datetime.now()
    time_for_clickhouse += end_time - start_time

print(f"For query: '{query}' x {query_count}, Total time: {time_for_clickhouse} in ClickHouse")
print(f"For query: '{query}' x {query_count}, Total time: {time_for_postgres} in Postgres")

clickhouse.disconnect()
cursor.close()
postgres.close()
