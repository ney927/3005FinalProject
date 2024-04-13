import psycopg2
import json

from db_secret import password
#this file is to teardown all tables in the database

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
print("deleting all tables...")

cursor.execute('''DROP SCHEMA public CASCADE;''')
cursor.execute('''CREATE SCHEMA public;''')

print("all tables have been deleted.")
conn.close()