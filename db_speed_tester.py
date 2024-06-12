import psycopg2
from datetime import datetime
from faker import Faker
from clickhouse_driver import Client

clickhouse = Client(host='localhost', port=9000, user='default', password='', database='default')

postgres = psycopg2.connect(host='localhost', port=5432, user='postgres', password='admin', database='postgres_speed_test')
cursor = postgres.cursor()

fake = Faker()

# data
# 63000094 rows in product 
# 1116311 rows in stockproduct
# 5 rows in stock

queries = [
            "SELECT * FROM product;",
            "SELECT * FROM product order by -id;",
            "SELECT * FROM product order by id;",
            "SELECT sum(price) FROM product;",
            "SELECT count(id) FROM product;",
            "select * from product order by price;",
            "select sum(amount) from stockproduct group by stock_id;",
            "select stock_id, product_id, sum(amount) as product_amount from stockproduct group by stock_id, product_id;",
            "select stock.name, product.name, sum(amount) as product_amount from stockproduct join product on stockproduct.product_id = product.id join stock on stockproduct.stock_id = stock.id group by stock.name, product.name;",
            "SELECT id, AVG(price) AS avg_price, MAX(price) AS max_price, MIN(price) AS min_price FROM product GROUP BY id;",
            "SELECT p.name, COUNT(sp.product_id) AS stock_count FROM product p LEFT JOIN stockproduct sp ON p.id = sp.product_id GROUP BY p.name;",
            "SELECT sp.stock_id, s.name AS stock_name, SUM(sp.amount) AS total_amount FROM stockproduct sp JOIN stock s ON sp.stock_id = s.id GROUP BY sp.stock_id, s.name;",
            "SELECT p.id, p.name, sp.stock_id, SUM(sp.amount) AS total_amount FROM product p JOIN stockproduct sp ON p.id = sp.product_id GROUP BY p.id, p.name, sp.stock_id HAVING SUM(sp.amount) > 100;",
            "SELECT sp.product_id, p.name AS product_name, COUNT(*) AS transaction_count, SUM(sp.amount) AS total_amount FROM stockproduct sp JOIN product p ON sp.product_id = p.id GROUP BY sp.product_id, p.name ORDER BY total_amount DESC;",
            "SELECT p.name, p.price, total_stock FROM product p LEFT JOIN (SELECT product_id, SUM(amount) AS total_stock FROM stockproduct GROUP BY product_id) sp ON p.id = sp.product_id ORDER BY total_stock DESC;",
            "SELECT p.name AS product_name, s.name AS stock_name, total_stock.total_amount FROM stockproduct sp JOIN product p ON sp.product_id = p.id JOIN stock s ON sp.stock_id = s.id LEFT JOIN (SELECT product_id, stock_id, SUM(amount) AS total_amount FROM stockproduct GROUP BY product_id, stock_id) total_stock ON sp.product_id = total_stock.product_id AND sp.stock_id = total_stock.stock_id GROUP BY p.name, s.name, total_stock.total_amount ORDER BY total_stock.total_amount DESC;",
            "SELECT p.id, p.name, p.price, SUM(sp.amount) OVER (PARTITION BY p.id) AS total_stock FROM product p JOIN stockproduct sp ON p.id = sp.product_id;",
            "SELECT * FROM product WHERE name = 'Alpha Widget';",
            "SELECT * FROM product WHERE name LIKE '%Widget%';",
            "SELECT * FROM product WHERE price BETWEEN 100 AND 500;",
            "SELECT * FROM product WHERE name IN ('Alpha Widget', 'Beta Widget', 'Gamma Widget');",
            "SELECT * FROM product WHERE name NOT IN ('Alpha Widget', 'Beta Widget', 'Gamma Widget');",
            "SELECT * FROM product WHERE name IS NULL;",
            "SELECT * FROM product WHERE name IS NOT NULL;",
            "SELECT * FROM product WHERE price > (SELECT AVG(price) FROM product);",
            "SELECT * FROM product WHERE name LIKE 'Alpha%' AND price > 100;",
            ]

# testing for select queries
count = 0
for query in queries:
    count += 1
    #clickhouse
    start_time = datetime.now()
    clickhouse.execute(query)
    end_time = datetime.now()
    clickhouse_time = (end_time - start_time).total_seconds()

    #postgres
    from_time = datetime.now()
    cursor.execute(query)
    to_time = datetime.now()
    postgres_time = (to_time - from_time).total_seconds()

    total_time = clickhouse_time + postgres_time

    clickhouse_percentage = (clickhouse_time / total_time) * 100
    postgres_percentage = (postgres_time / total_time) * 100

    print(f"{count}). For query: '{query}' Total time: {end_time - start_time} in ClickHouse {clickhouse_percentage:.2f}%")
    print(f"{count}). For query: '{query}' Total time: {to_time - from_time} in Postgres {postgres_percentage:.2f}%")
    print("\n")



# close connections
postgres.close()
cursor.close()
clickhouse.disconnect()
