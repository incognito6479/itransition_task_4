from sqlalchemy import create_engine
from dotenv import load_dotenv
import os, psycopg2


load_dotenv()
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv("DB_NAME")
connection_params = {
    'host': 'localhost',        
    'database': db_name,
    'user': db_user,
    'password': db_password,
    'port': 5432
}

def checkRowCount(table_name):
    query = f'SELECT count(*) FROM {table_name}'
    conn = psycopg2.connect(**connection_params)
    cur = conn.cursor()
    cur.execute(query)
    row_count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return row_count

def loadIntoDB(df, table_name):
    if checkRowCount(table_name) == 0:
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@127.0.0.1:5432/{db_name}')
        df.to_sql(table_name, engine, if_exists="append", index=False)