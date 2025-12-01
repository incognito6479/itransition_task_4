from flask import Flask, render_template
import psycopg2, os 


connection_params = {
    'host': 'localhost',        
    'database': os.getenv("DB_NAME"),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': 5432
}

app = Flask(__name__)

def db_connect():
    conn = psycopg2.connect(**connection_params)
    return conn 

@app.route("/")
def mainPage():
    return render_template("index.html")

if __name__ == "__main__":
    app.run("127.0.0.1", port=5000, debug=True)