import psycopg2
import json

from db_secret import password

#this file is to teardown all tables in the database

conn = psycopg2.connect(
    database = 'soccer',
    user = 'postgres',
    password = password,
    host = 'localhost',
    port = '5432'
)

conn.autocommit = True
cursor = conn.cursor()
print("databse connected!")

url = './data'
competitions_json = json.load(open(url+'/competitions.json', 'r', encoding='utf-8'))

def create_country_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Countrys(
                   id INT UNIQUE NOT NULL PRIMARY KEY,
                   name VARCHAR(255)
    )''')

def create_referee_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Referees(
                   id INT UNIQUE NOT NULL PRIMARY KEY,
                   name VARCHAR(255),
                   country_id INT,
                   FOREIGN KEY (country_id) REFERENCES Countrys (id)

    )''')

def create_stadium_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Stadiums(
                   id INT UNIQUE NOT NULL PRIMARY KEY,
                   name VARCHAR(255),
                   country_id INT,
                   FOREIGN KEY (country_id) REFERENCES Countrys (id)

    )''')


create_country_table()
print("create_country_table() successful!")
create_referee_table()
print("create_referee_table() successful!")
create_stadium_table()
print("create_stadium_table() successful!")

conn.close()