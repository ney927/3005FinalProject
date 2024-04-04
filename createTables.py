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
    print("create_country_table() successful!")

def create_referee_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Referees(
                   id INT UNIQUE NOT NULL PRIMARY KEY,
                   name VARCHAR(255),
                   country_id INT,
                   FOREIGN KEY (country_id) REFERENCES Countrys (id)

    )''')
    print("create_referee_table() successful!")

def create_stadium_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Stadiums(
                   id INT UNIQUE NOT NULL PRIMARY KEY,
                   name VARCHAR(255),
                   country_id INT,
                   FOREIGN KEY (country_id) REFERENCES Countrys (id)

    )''')
    print("create_stadium_table() successful!")

def create_team_table():
    #group cannot just be group as that is a sql keyword so it is team_group
    cursor.execute('''CREATE TABLE IF NOT EXISTS Teams(
                   id INT UNIQUE NOT NULL PRIMARY KEY,
                   team_name VARCHAR(255) NOT NULL,
                   gender VARCHAR(255) NOT NULL,
                   team_group VARCHAR(255) NOT NULL

    )''')
    print("create_team_table() successful!")

def create_manager_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Managers(
                   id INT UNIQUE NOT NULL PRIMARY KEY,
                   name VARCHAR(255) NOT NULL,
                   nickname VARCHAR(255),
                   date_of_birth DATE,
                   country_id INT NOT NULL,
                   team_id INT NOT NULL,
                   FOREIGN KEY (country_id) REFERENCES Countrys (id),
                   FOREIGN KEY (team_id) REFERENCES Teams (id)


    )''')
    print("create_manager_table() successful!")

def create_all_db_tables():
    #ordered such that tables are created before their possible foreign keys are referenced
    create_country_table()

    create_referee_table()

    create_stadium_table()

    create_team_table()

    create_manager_table()


create_all_db_tables()

conn.close()