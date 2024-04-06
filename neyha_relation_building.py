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

url = './data'

# TODO -> THE SCHEMA FOR CREATE TABLE STATEMENT IS INCOMPLETED (missing stuff like unique, null, primary key, etc.)
def create_competitions_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Competitions(
                competition_id INT NOT NULL,
                season_id INT NOT NULL,
                country_name VARCHAR(255),
                competition_name VARCHAR(255),
                competition_gender VARCHAR(255),
                competition_youth BOOL,
                competition_international BOOL,
                season_name VARCHAR(255),
                PRIMARY KEY (competition_id, season_id)
    )''')
    print("create_competitions_table() successful!")


def create_matches_table():
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
                PRIMARY KEY (match_id),
                FOREIGN KEY (competition_id, season_id) references Competitions (competition_id, season_id)
                        on delete set null,
                FOREIGN KEY (home_team_id) references Teams (id)
                        on delete set null,
                FOREIGN KEY (away_team_id) references Teams (id)
                        on delete set null,
                FOREIGN KEY (stadium_id) references Stadiums (id)
                        on delete set null,
                FOREIGN KEY (referee_id) references Referees (id)
                        on delete set null
    )''')
    print("create_matches_table() successful!")


def insert_competitions_data():
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

    # insert competition data
    cursor.execute(insert_competitions)
    print("done insert_competitions_date()")


def insert_matches_data():
    match_attributes = ['match_id', 'match_date', 'kick_off', 'home_score', 'away_score', 'match_week']
    # match_attribue_fk = [ [attribute_object_name, attribute_id_name], .. ]
    match_attribute_fk = [['competition', 'competition_id'], ['season', 'season_id'], ['home_team', 'home_team_id'], ['away_team', 'away_team_id'], ['competition_stage', 'id'], ['stadium', 'id'], ['referee', 'id']]
    insert_matches = 'INSERT INTO Matches (match_id, match_date, kick_off, home_score, away_score, match_week, competition_id, season_id, home_team_id, away_team_id, competition_stage_id, stadium_id, referee_id) VALUES '
    for season in os.listdir(url+'/matches'): 
        for match_id in os.listdir(url+'/matches/'+season):
            match_json = json.load(open(url+'/matches/'+season+'/'+match_id, 'r', encoding='utf-8'))
            for match in match_json:
                insertMatch = '('
                for attribute in match_attributes:
                    value = str(match[attribute])
                    insertMatch += '\'' + value + '\','

                for attribute in match_attribute_fk:
                    if(attribute[0] in match):
                        value = str(match[attribute[0]][attribute[1]])
                        insertMatch += '\'' + value + '\','
                    else:
                        insertMatch += 'NULL,'
                insertMatch = insertMatch[:-1] + '),'
                insert_matches += insertMatch

    insert_matches = insert_matches[:-1] + ''

    cursor.execute(insert_matches)  
    print("done insert_matches_data()")



# cursor.execute('DROP TABLE IF EXISTS Matches') #DELETES Matches TABLE
# create_matches_table()
# insert_matches_data()
    
# cursor.execute('DROP TABLE IF EXISTS Competitions') #DELETES Competitions TABLE
# create_competitions_table()
# insert_competitions_data()
    

#lineup stuff
    
def create_positions_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Positions(
                id SERIAL,
                position_id INT,
                position_name VARCHAR(255),
                from_time VARCHAR(255),
                to_time VARCHAR(255),
                from_period INT,
                to_period INT,
                start_reason VARCHAR(255),
                end_reason VARCHAR(255),
                player_id INT,
                PRIMARY KEY (id),
                FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create_positions_table() successful!")


def insert_position_data():
    position_attributes = ['position_id', 'position', 'from', 'to', 'from_period', 'to_period', 'start_reason', 'end_reason']
    insert_positions = 'INSERT INTO Positions (position_id, position_name, from_time, to_time, from_period, to_period, start_reason, end_reason, player_id) VALUES '
    for matchid in os.listdir(url+"/lineups"):
        lineups_json = json.load(open(url+"/lineups/"+matchid, "r", encoding="utf-8"))
        for lineup in lineups_json:
            for player in lineup['lineup']:
                for position in player['positions']:
                    insertPos = '(' 
                    for attribute in position_attributes:
                        value = str(position[attribute])
                        value = value.replace('\'', '\'\'')
                        if value == 'None':
                            insertPos += 'NULL,'    
                        else:
                            insertPos += '\'' + value + '\','    
                    insertPos += str(player['player_id']) + '),\n'
                    insert_positions += insertPos

    insert_positions = insert_positions[:-2] + ''
    cursor.execute(insert_positions)
    # print(insert_positions)
    print("done insert_positions_date()")


# cursor.execute('DROP TABLE IF EXISTS Positions') #DELETES Positions TABLE
# create_positions_table()
# insert_position_data()

conn.close()

