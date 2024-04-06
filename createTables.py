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

def create_player_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Players(
                   id INT UNIQUE NOT NULL PRIMARY KEY,
                   name VARCHAR(255) NOT NULL,
                   nickname VARCHAR(255),
                   jersey_number INT NOT NULL,
                   country_id INT NOT NULL,
                   team_id INT NOT NULL,
                   FOREIGN KEY (country_id) REFERENCES Countrys (id),
                   FOREIGN KEY (team_id) REFERENCES Teams (id)


    )''')
    print("create_player_table() successful!")

def create_events_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Events(
                   id VARCHAR(255) UNIQUE NOT NULL PRIMARY KEY,
                   index INT NOT NULL,
                   period INT NOT NULL,
                   timestamp TIME NOT NULL,
                   minute INT NOT NULL,
                   second INT NOT NULL,
                   type VARCHAR(255) NOT NULL,
                   possession INT NOT NULL,
                   possesstion_team INT NOT NULL,
                   play_pattern VARCHAR(255) NOT NULL,
                   team INT NOT NULL,
                   duration numeric,
                   tactics VARCHAR(255),
                   FOREIGN KEY (possesstion_team) REFERENCES Teams (id)


    )''')
    print("create_events_table() successful!")

def create_miscontrol_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Miscontrol(
                   id VARCHAR(255) UNIQUE NOT NULL PRIMARY KEY,
                   areial_won boolean NOT NULL,
                   player_id INT NOT NULL,
                   FOREIGN KEY (id) REFERENCES Events (id),
                   FOREIGN KEY (player_id) REFERENCES Players (id)



    )''')
    print("create_miscontrol_table() successful!")

def create_playerOff_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Player_Off(
                   id VARCHAR(255) UNIQUE NOT NULL PRIMARY KEY,
                   permanent boolean NOT NULL,
                   player_id INT NOT NULL,
                   FOREIGN KEY (id) REFERENCES Events (id),
                   FOREIGN KEY (player_id) REFERENCES Players (id)



    )''')
    print("create_player_Off_table() successful!")

def create_pressure_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Pressure(
                   id VARCHAR(255) UNIQUE NOT NULL PRIMARY KEY,
                   counterpress boolean NOT NULL,
                   player_id INT NOT NULL,
                   FOREIGN KEY (id) REFERENCES Events (id),
                   FOREIGN KEY (player_id) REFERENCES Players (id)



    )''')
    print("create_pressure_table() successful!")

def create_all_db_tables():
    #ordered such that tables are created before their possible foreign keys are referenced
    create_country_table()

    create_referee_table()

    create_stadium_table()

    create_team_table()

    create_manager_table()

    create_player_table()

    create_events_table()
    #Below are events
    create_miscontrol_table()
    create_playerOff_table()
    create_pressure_table()


create_all_db_tables()

conn.close()