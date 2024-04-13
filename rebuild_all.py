import psycopg2
import json

from db_secret import password

from neyha_relation_building import *
from insert_data import *
from createTables import *

#NOTE -> this file does not build / teardown any events tables

conn = psycopg2.connect(
    database = 'project_database',
    user = 'postgres',
    password = password,
    host = 'localhost',
    port = '5432'
)

conn.autocommit = True
cursor = conn.cursor()
print("databse connected!")

url = './data'

cursor.execute('DROP TABLE IF EXISTS Competitions') 
create_competitions_table()
insert_competitions_data()

cursor.execute('DROP TABLE IF EXISTS Matches') 
create_matches_table()
insert_matches_data()

cursor.execute('DROP TABLE IF EXISTS Countrys') 
create_country_table()
insert_country_data()
insert_country2_data()

cursor.execute('DROP TABLE IF EXISTS Referees') 
create_referee_table()
insert_referee_data()

cursor.execute('DROP TABLE IF EXISTS Stadiums') 
create_stadium_table()
insert_stadium_data()

cursor.execute('DROP TABLE IF EXISTS Teams') 
create_team_table()
insert_team_data()

cursor.execute('DROP TABLE IF EXISTS Managers') 
create_manager_table()
insert_manager_data()

cursor.execute('DROP TABLE IF EXISTS Players') 
create_player_table()
insert_player_data()

cursor.execute('DROP TABLE IF EXISTS Positions') 
create_positions_table()
insert_positions_data()


