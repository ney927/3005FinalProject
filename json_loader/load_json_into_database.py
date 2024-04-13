
from createTables import create_all_db_tables;
from insert_data import insert_all_data;
from db_secret import password
import psycopg2
import json
import os

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


create_all_db_tables()
insert_all_data()

conn.close()

print('done loading json into database')