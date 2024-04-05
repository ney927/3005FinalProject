import psycopg2
import json
import os

#this stuff is importing my postgres password which is in a file on my laptop and not in the git
#change password var to your password if you want this to connect to your postgres
#also it won't work if you haven't created a soccer db in pg4admin
#this can just be used as a reference of how to connect to db
from db_secret import password

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

def printRelation(relation):
    cursor.execute(f'SELECT * from {relation}')
    rows = cursor.fetchall()
    for row in rows:
        print(row)

# creates and fills the Competitions table
# IMPORTANT!! THIS WILL DELETE THE COMPETITIONS TABLE AFTER MAKING IT
# TODO -> THE SCHEMA FOR CREATE TABLE STATEMENT IS INCOMPLETED (missing stuff like unique, null, primary key, etc.)
    #  -> should the int values not be cast into strings?
url = './data'
competitions_json = json.load(open(url+'/competitions.json', 'r', encoding='utf-8'))

# generate the sql statement to insert competition data into the relation
competition_attributes = ['competition_id', 'season_id', 'country_name', 'competition_name', 'competition_gender', 'competition_youth', 'competition_international', 'season_name']
insert_competitions = 'INSERT INTO Competitions (competition_id, season_id, country_name, competition_name, competition_gender, competition_youth, competition_international, season_name) VALUES '
for comp in competitions_json:
    insertComp = '('
    for attribute in competition_attributes:
        value = str(comp[attribute])
        value = value.replace('\'', '\'\'')
        insertComp += '\'' + value + '\','
    insertComp = insertComp[:-1] + '),'
    insert_competitions += insertComp
insert_competitions = insert_competitions[:-1] + ''

# cursor.execute('DROP TABLE IF EXISTS Competitions') #DELETES COMPETITION TABLE
# # create competition table
# cursor.execute('''CREATE TABLE IF NOT EXISTS Competitions(
#                competition_id INT NOT NULL,
#                season_id INT NOT NULL,
#                country_name VARCHAR(255),
#                competition_name VARCHAR(255),
#                competition_gender VARCHAR(255),
#                competition_youth BOOL,
#                competition_international BOOL,
#                season_name VARCHAR(255),
#                PRIMARY KEY (competition_id, season_id)
# )''')

# # insert competition data
# cursor.execute(insert_competitions)

# #print competition data
# printRelation('Competitions')


#matches
match_attributes = ['match_id', 'match_date', 'kick_off', 'home_score', 'away_score', 'match_week']
# match_attribue_fk = [ [attribute_object_name, attribute_id_name], .. ]
match_attribute_fk = [['competition', 'competition_id'], ['season', 'season_id'], ['home_team', 'home_team_id'], ['away_team', 'away_team_id'], ['competition_stage', 'id'], ['stadium', 'id'], ['referee', 'id']]
insert_matches = 'INSERT INTO Matches (match_id, match_date, kick_off, home_score, away_score, match_week, competition_stage_id, season_id, home_team_id, away_team_id, competition_id, stadium_id, referee_id) VALUES '
for season in os.listdir(url+'/matches'): 
    for match_id in os.listdir(url+'/matches/'+season):
        match_json = json.load(open(url+'/matches/'+season+'/'+match_id, 'r', encoding='utf-8'))
        for match in match_json:
            insertMatch = '('
            if match['competition']['competition_id'] == '1':
                print("Found a competition with id=1")
            for attribute in match_attributes:
                value = str(match[attribute])
                insertMatch += '\'' + value + '\','

            for attribute in match_attribute_fk:
                if(attribute[0] in match):
                    value = str(match[attribute[0]][attribute[1]])
                    insertMatch += '\'' + value + '\','
                else:
                    insertMatch += 'NULL,'
            insertMatch = insertMatch[:-1] + '),\n'
            insert_matches += insertMatch

insert_matches = insert_matches[:-2] + '' #change back to one when u get rid of \n on line 92 

cursor.execute('DROP TABLE IF EXISTS Matches') #DELETES Matches TABLE

#this order should be the same as insert_matches
cursor.execute('''CREATE TABLE IF NOT EXISTS Matches (
               match_id INT NOT NULL UNIQUE,
               match_date DATE NOT NULL,
               kick_off VARCHAR(255),
               home_score INT NOT NULL,
               away_score INT NOT NULL,
               match_week INT NOT NULL,
               competition_id INT NOT NULL,
               season_id INT,
               home_team_id INT NOT NULL,
               away_team_id INT NOT NULL,
               competition_stage_id INT,
               stadium_id INT,
               referee_id INT,
               PRIMARY KEY (match_id)
)''')
            #    FOREIGN KEY (competition_id, season_id) references Competitions (competition_id, season_id)
		    #         on delete set null
print(insert_matches)
cursor.execute(insert_matches)

# printRelation('Matches')


conn.close()

