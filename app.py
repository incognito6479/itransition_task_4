from flask import Flask, render_template
from reprocess import reprocessBooks
from db_init import connection_params
import psycopg2


app = Flask(__name__)

def db_connect():
    conn = psycopg2.connect(**connection_params)
    return conn 

@app.route("/")
def mainPage():
    reprocessBooks()
    return render_template("index.html")

if __name__ == "__main__":
    app.run("127.0.0.1", port=5000, debug=True)