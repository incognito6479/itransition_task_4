import psycopg2, os 
from dotenv import load_dotenv


load_dotenv()

connection_params = {
    'host': 'localhost',        
    'database': os.getenv("DB_NAME"),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': 5432
}

connection = psycopg2.connect(**connection_params)

cur = connection.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS books (
        id                  BIGINT PRIMARY KEY,
        title               TEXT,
        author              TEXT,
        genre               TEXT,
        publisher           TEXT,
        year                INTEGER,
        data_source         VARCHAR(5) 
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id                  BIGINT PRIMARY KEY,
        name                TEXT,
        address             TEXT,
        phone               TEXT,
        email               TEXT,
        data_source         VARCHAR(5) 
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id                  BIGINT PRIMARY KEY,
        user_id             INTEGER REFERENCES users(id),
        book_id             INTEGER REFERENCES books(id),
        quantity            INTEGER,
        unit_price          DECIMAL(10, 2),
        paid_price          DECIMAL(10, 2),
        timestamp           TIMESTAMP,
        shipping            TEXT,
        data_source         VARCHAR(5),
        currency_type       VARCHAR(3)
    )
""")

cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_books_data_source ON books (data_source)
""")

cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_users_data_source ON users (data_source)
""")

cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_orders_data_source ON orders (data_source)
""")

cur.close()
connection.commit()
connection.close()