import psycopg2
import json
import os

#this stuff is importing my postgres password which is in a file on my laptop and not in the git
#change password var to your password if you want this to connect to your postgres
#also it won't work if you haven't created a soccer db in pg4admin
#this can just be used as a reference of how to connect to db
from db_secret import password

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

def printRelation(relation):
    cursor.execute(f'SELECT * from {relation}')
    rows = cursor.fetchall()
    for row in rows:
        print(row)

# url = './open-data/data'
url = 'data'

# TODO -> THE SCHEMA FOR CREATE TABLE STATEMENT IS INCOMPLETED (missing stuff like unique, null, primary key, etc.)
def create_competitions_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS Competitions(
                competition_id INT NOT NULL,
                season_id INT NOT NULL,
                country_name VARCHAR(255) NOT NULL ,
                competition_name VARCHAR(255) NOT NULL ,
                competition_gender VARCHAR(255) NOT NULL ,
                competition_youth BOOL NOT NULL ,
                competition_international BOOL NOT NULL ,
                season_name VARCHAR(255) NOT NULL ,
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
    insert_competitions = insert_competitions[:-1] + ' ON CONFLICT DO NOTHING'

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
                id SERIAL NOT NULL UNIQUE,
                position_id INT NOT NULL,
                position_name VARCHAR(255) NOT NULL,
                from_time VARCHAR(255),
                to_time VARCHAR(255),
                from_period INT,
                to_period INT,
                start_reason VARCHAR(255),
                end_reason VARCHAR(255),
                player_id INT NOT NULL,
                match_id INT NOT NULL,
                PRIMARY KEY (id),
                FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create_positions_table() successful!")


def insert_positions_data():
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
                    insertPos += str(player['player_id']) + ','+ str(matchid) + '),'
                    insert_positions += insertPos

    insert_positions = insert_positions[:-1] + ''
    cursor.execute(insert_positions)
    # print(insert_positions)
    print("done insert_positions_date()")


# cursor.execute('DROP TABLE IF EXISTS Positions') #DELETES Positions TABLE
# create_positions_table()
# insert_positions_data()
    


#event stuff
#neyha
# "50/50"
# "Bad Behaviour"
# "Ball Receipt*"
# "Ball Recovery"
# "Block"
# "Carry"
# "Clearance"
# "Dribble"
# "Dribbled Past"
# "Duel"
# "Foul Committed"
# "Foul Won"
# "Goal Keeper"
# "Half End"
# "Half Start"
# "Injury Stoppage"
# "Interception"
    
def delete_event_type_tables():
    cursor.execute("DROP TABLE IF EXISTS Ball_Receipt")
    cursor.execute("DROP TABLE IF EXISTS Ball_Recovery")
    cursor.execute("DROP TABLE IF EXISTS Block")
    cursor.execute("DROP TABLE IF EXISTS Injury_Stoppage")
    cursor.execute("DROP TABLE IF EXISTS event_5050")
    cursor.execute("DROP TABLE IF EXISTS bad_behaviour")
    cursor.execute("DROP TABLE IF EXISTS dribbled_past")
    cursor.execute("DROP TABLE IF EXISTS half_end")
    cursor.execute("DROP TABLE IF EXISTS half_start")
    cursor.execute("DROP TABLE IF EXISTS carry")
    cursor.execute("DROP TABLE IF EXISTS clearance")
    cursor.execute("DROP TABLE IF EXISTS dribble")
    cursor.execute("DROP TABLE IF EXISTS duel")
    cursor.execute("DROP TABLE IF EXISTS foul_committed")
    cursor.execute("DROP TABLE IF EXISTS foul_won")
    cursor.execute("DROP TABLE IF EXISTS goal_keeper")
    cursor.execute("DROP TABLE IF EXISTS interception")
    
def create_event_type_tables():
    # "50/50", "Bad Behaviour", "Ball Receipt*", "Ball Recovery", "Block", "Carry", "Clearance", "Dribble", "Dribbled Past"
    # "Duel", "Foul Committed", "Foul Won", "Goal Keeper", "Half End", "Half Start", "Injury Stoppage", "Interception"
    cursor.execute('''CREATE TABLE IF NOT EXISTS event_5050(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   outcome VARCHAR(255),
                   counterpress BOOL NOT NULL,
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create 5050 table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Bad_Behaviour (
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   card VARCHAR(255),
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create Bad_Behaviour table was successful!")
    
    cursor.execute(''' CREATE TABLE IF NOT EXISTS Ball_Receipt (
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   outcome VARCHAR(255),
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')    
    print("create Ball_Receipt table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Ball_Recovery(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   offensive BOOL NOT NULL,
                   recovery_failure BOOL NOT NULL,
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create Ball_Recovery table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Block(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   deflection BOOL NOT NULL,
                   offensive BOOL NOT NULL,
                   save_block BOOL NOT NULL,
                   counterpress BOOL NOT NULL,
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create Block table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Carry(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   under_pressure BOOL,
                   end_location_x FLOAT(1) NOT NULL,
                   end_location_y FLOAT(1) NOT NULL,
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create Carry table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Clearance(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   under_pressure BOOL NOT NULL,
                   aerial_won BOOL NOT NULL,
                   body_part VARCHAR(255), 
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create Clearance table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Dribble(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   nutmeg BOOL,
                   outcome VARCHAR(255) NOT NULL,
                   no_touch BOOL NOT NULL,
                   overrun BOOL NOT NULL,
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create Dribble table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Dribbled_Past(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   no_touch BOOL NOT NULL,
                   overrun BOOL NOT NULL,
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create Dribbled_Past table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Duel(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   type VARCHAR(255) NOT NULL,
                   outcome VARCHAR(255),
                   under_pressure BOOL NOT NULL,
                   counterpress BOOL NOT NULL,
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create Duel table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Foul_Committed(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   counterpress BOOL NOT NULL,
                   offensive BOOL NOT NULL,
                   advantage BOOL NOT NULL,
                   penalty BOOL NOT NULL,
                   card VARCHAR(255),
                   type VARCHAR(255),
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create Foul_Committed table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Foul_Won(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   defensive BOOL NOT NULL,
                   advantage BOOL NOT NULL,
                   penalty BOOL NOT NULL,
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create Foul_Won table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Goal_Keeper(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   end_location_x FLOAT(1),
                   end_location_y FLOAT(1),
                   goal_position VARCHAR(255),
                   outcome VARCHAR(255),
                   type VARCHAR(255),
                   body_part VARCHAR(255),
                   technique VARCHAR(255),
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create Goal_Keeper table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Half_End(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   season_id INT NOT NULL,
                   early_video_end BOOL NOT NULL,
                   match_suspended BOOL NOT NULL,
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null
    )''')
    print("create Half_End table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Half_Start(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   season_id INT NOT NULL,
                   late_video_start BOOL NOT NULL,
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null
    )''')
    print("create Half_Start table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Injury_Stoppage(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   in_chain BOOL NOT NULL,
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create Injury_Stoppage table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Interception(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
                   outcome VARCHAR(255) NOT NULL,
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null,
                   FOREIGN KEY (player_id) references Players (id)
                        on delete set null
    )''')
    print("create Interception table was successful!")


    
def insert_events_type_data():
    directory = url + '/events'
    filenum = 0
    insert_ball_receipt = "INSERT INTO Ball_Receipt (id, player_id, position_name, season_id, outcome) VALUES "
    insert_ball_recovery = "INSERT INTO Ball_Recovery (id, player_id, position_name, season_id, offensive, recovery_failure) VALUES "
    insert_block = "INSERT INTO block (id, player_id, position_name, season_id, deflection, offensive, save_block, counterpress) VALUES "
    insert_injury_stoppage = "INSERT INTO injury_stoppage (id, player_id, position_name, season_id, in_chain) VALUES "
    insert_5050 = "INSERT INTO event_5050 (id, player_id, position_name, season_id, outcome, counterpress) VALUES "
    insert_bad_behaviour = "INSERT INTO bad_behaviour (id, player_id, position_name, season_id, card) VALUES "
    insert_dribbled_past = "INSERT INTO dribbled_past (id, player_id, position_name, season_id, no_touch, overrun) VALUES "
    insert_half_end = "INSERT INTO half_end (id, season_id, early_video_end, match_suspended) VALUES "
    insert_half_start = "INSERT INTO half_start (id, season_id, late_video_start) VALUES "
    insert_carry = "INSERT INTO carry (id, player_id, position_name, season_id, under_pressure, end_location_x, end_location_y) VALUES "
    insert_clearance = "INSERT INTO clearance (id, player_id, position_name, season_id, under_pressure, aerial_won, body_part) VALUES "
    insert_dribble = "INSERT INTO dribble (id, player_id, position_name, season_id, nutmeg, outcome, no_touch, overrun) VALUES "
    insert_duel = "INSERT INTO duel (id, player_id, position_name, season_id, type, under_pressure, outcome, counterpress) VALUES "
    insert_foul_committed = "INSERT INTO foul_committed (id, player_id, position_name, season_id, counterpress, offensive, advantage, penalty, card, type) VALUES "
    insert_foul_won = "INSERT INTO foul_won (id, player_id, position_name, season_id, defensive, advantage, penalty) VALUES "
    insert_goal_keeper = "INSERT INTO goal_keeper (id, player_id, position_name, season_id, end_location_x, end_location_y, goal_position, outcome, type, body_part, technique) VALUES "
    insert_interception = "INSERT INTO interception (id, player_id, position_name, season_id, outcome) VALUES "
    #go through each file in lineups folder
    for filename in os.listdir(directory):
        individual_match_json = json.load(open(directory + '/' + filename, 'r', encoding='utf-8'))
        cursor.execute("SELECT season_id FROM matches WHERE matches.match_id="+filename[:-5]+";")
        season_id = str(cursor.fetchone()[0])
    
        #for each event in a json file
        for event in individual_match_json:
            type = event['type']['name']
            basic_insert = ""
            if 'player' in event:
                basic_insert = "(\'"+str(event['id'])+"\',\'"+str(event['player']['id'])+"\',\'"+event['position']['name']+"\',\'"+season_id+"\'),"
            else:
                basic_insert = "(\'"+str(event['id'])+"\',\'"+season_id+"\'),"
            
            # Ball Receipt
            "Ball Receipt*"
            if type == "Ball Receipt*":
                if 'ball_receipt' in event:
                    insert_ball_receipt += basic_insert[:-2]+",\'"+event['ball_receipt']['outcome']['name']+"\'),"
                else:
                    insert_ball_receipt += basic_insert[:-2]+",NULL),"
            
            # "Ball Recovery"
            elif type == "Ball Recovery":
                insert_ball_recovery += basic_insert[:-2]

                if 'ball_reinsert_ball_recovery' in event:
                    bool_attr = ['offensive', 'recovery_failure']
                    for attr in bool_attr:
                        if attr in event['ball_reinsert_ball_recovery']:
                            insert_ball_recovery += ",\'"+str(event['ball_reinsert_ball_recovery'][attr])+"\'"
                        else: 
                            insert_ball_recovery += ",\'false\'"
                else: 
                    insert_ball_recovery += ",\'false\',\'false\'"
            
                insert_ball_recovery += "),"

            # "Block"
            elif type == "Block":
                insert_block += basic_insert[:-2]

                if 'block' in event:
                    bool_attr = ['deflection', 'offensive', 'save_block', 'counterpress']
                    for attr in bool_attr:
                        if attr in event['block']:
                            insert_block += ",\'"+str(event['block'][attr])+"\'"
                        else: 
                            insert_block += ",\'false\'"
                else: 
                    insert_block += ",\'false\',\'false\',\'false\',\'false\'"
            
                insert_block += "),"

            # "Dribbled Past"
            elif type == "Dribbled Past":
                insert_dribbled_past += basic_insert[:-2]

                if 'dribbled_past' in event:
                    bool_attr = ['no_touch', 'overrun']
                    for attr in bool_attr:
                        if attr in event['dribbled_past']:
                            insert_dribbled_past += ",\'"+str(event['dribbled_past'][attr])+"\'"
                        else: 
                            insert_dribbled_past += ",\'false\'"
                else: 
                    insert_dribbled_past += ",\'false\',\'false\'"
            
                insert_dribbled_past += "),"

            # "Injury Stoppage"
            elif type == "Injury Stoppage":
                if basic_insert.count(',') == 4:
                    insert_injury_stoppage += basic_insert[:-2]
                    if 'injury_stoppage' in event:
                        if 'in_chain' in event['injury_stoppage']:
                            insert_injury_stoppage += ",\'"+str(event['injury_stoppage']['in_chain'])+"\'"
                        else: 
                            insert_injury_stoppage += ",\'false\'"
                    else: 
                        insert_injury_stoppage += ",\'false\'"
                
                    insert_injury_stoppage += "),"

                
            # "50/50"
            elif type == "50/50":
                insert_5050 += basic_insert[:-2]

                if '50_50' in event:
                    if 'outcome' in event['50_50']: 
                        insert_5050 += ",\'"+str(event['50_50']['outcome']['name'])+"\'"
                    else:
                        insert_5050 += ",NULL"

                    if 'counterpress' in event['50_50']:
                        insert_5050 += ",\'"+str(event['50_50']['counterpress'])+"\'"
                    else: 
                        insert_5050 += ",\'false\'"
                else: 
                    insert_5050 += ",NULL,\'false\'"
            
                insert_5050 += "),"

            # "Bad Behaviour"
            elif type == "Bad Behaviour":
                insert_bad_behaviour += basic_insert[:-2]
                if 'bad_behaviour' in event:
                    if 'card' in event['bad_behaviour']:
                        insert_bad_behaviour += ",\'"+str(event['bad_behaviour']['card']['name'])+"\'"
                    else: 
                        insert_bad_behaviour += ",NULL"
                insert_bad_behaviour += "),"
            
            # "Half End"
            elif type == "Half End":
                insert_half_end += basic_insert[:-2]

                if 'half_end' in event:
                    bool_attr = ['early_video_end', 'match_suspended']
                    for attr in bool_attr:
                        if attr in event['half_end']:
                            insert_half_end += ",\'"+str(event['half_end'][attr])+"\'"
                        else: 
                            insert_half_end += ",\'false\'"
                else: 
                    insert_half_end += ",\'false\',\'false\'"
            
                insert_half_end += "),"
            
            # "Half Start"
            elif type == "Half Start":
                insert_half_start += basic_insert[:-2]

                if 'half_start' in event:
                    insert_half_start += ",\'"+str(event['half_start']['late_video_start'])+"\'"
                else: 
                    insert_half_start += ",\'false\'"
            
                insert_half_start += "),"

            # "Carry"
            if type == "Carry":
                if 'under_pressure' in event:
                    pressure = "\'"+str(event['under_pressure'])+"\'"
                else:
                    pressure = "NULL"
                insert_carry += basic_insert[:-2]+","+pressure+",\'"+str(event['carry']['end_location'][0])+"\',\'"+str(event['carry']['end_location'][1])+"\'),"
                
            # "Clearance"
            elif type == "Clearance":
                insert_clearance += basic_insert[:-2]+",\'"+str(event['under_pressure'])+"\'"
                if 'clearance' in event:
                    if 'aerial_won' in event['clearance']:
                        insert_clearance += ",\'"+str(event['clearance']['aerial_won'])+"\'"
                    else: 
                        insert_clearance += ",\'false\'"
                    if 'body_part' in event['clearance']:
                        insert_clearance += ",\'"+str(event['clearance']['body_part']['name'])+"\'"
                    else: 
                        insert_clearance += ",NULL"
                else:
                    insert_clearance += ",\'false\',NULL"
                insert_clearance += "),"
                

            # "Dribble"
            elif type == "Dribble":
                if 'nutmeg' in event['dribble']:
                    insert_dribble += basic_insert[:-2]+",\'"+str(event['dribble']['nutmeg'])+"\',\'"+event['dribble']['outcome']['name']+"\'"
                else:
                    insert_dribble += basic_insert[:-2]+",NULL,\'"+event['dribble']['outcome']['name']+"\'"

                if 'dribble' in event:
                    bool_attr = ['no_touch', 'overrun']
                    for attr in bool_attr:
                        if attr in event['dribble']:
                            insert_dribble += ",\'"+str(event['dribble'][attr])+"\'"
                        else: 
                            insert_dribble += ",\'false\'"
                else: 
                    insert_dribble += ",\'false\',\'false\'"
            
                insert_dribble += "),"
            
            # # "Duel"
            elif type == "Duel":
                insert_duel += basic_insert[:-2]+",\'"+event['duel']['type']['name']+"\',\'"+str(event['under_pressure'])+"\'"
                if 'outcome' in event['duel']:
                    insert_duel += ",\'"+event['duel']['outcome']['name']+"\'"
                else: 
                    insert_duel += ",NULL"
                if 'counterpress' in event:
                    insert_duel += ",\'"+str(event['counterpress'])+"\'),"
                else: 
                    insert_duel += ",\'false\'),"
                    
            
            # # "Foul Committed"
            elif type == "Foul Committed":
                insert_foul_committed += basic_insert[:-2]
                if 'counterpress' in event:
                    insert_foul_committed += ",\'"+str(event['counterpress'])+"\'"
                else: 
                    insert_foul_committed += ",\'false\'"

                if 'foul_committed' in event:
                    # check advantage, offensive, card['name'], penalty, type['name']
                    bool_attr = ['advantage', 'offensive', 'penalty']
                    for attr in bool_attr:
                        if attr in event['foul_committed']:
                            insert_foul_committed += ",\'"+str(event['foul_committed'][attr])+"\'"
                        else: 
                            insert_foul_committed += ",\'false\'"
                    if 'card' in event['foul_committed']:
                        insert_foul_committed += ",\'"+str(event['foul_committed']['card']['name'])+"\'"
                    else: 
                        insert_foul_committed += ",NULL"
                    if 'type' in event['foul_committed']:
                        insert_foul_committed += ",\'"+str(event['foul_committed']['type']['name'])+"\'"
                    else: 
                        insert_foul_committed += ",NULL"
                else: 
                    insert_foul_committed += ",\'false\',\'false\',\'false\',NULL,NULL"

                
                insert_foul_committed += "),"
            
            # # "Foul Won"
            elif type == "Foul Won":
                insert_foul_won += basic_insert[:-2]

                if 'foul_won' in event:
                    # check advantage, defensive, penalty
                    bool_attr = ['advantage', 'offensive', 'penalty']
                    for attr in bool_attr:
                        if attr in event['foul_won']:
                            insert_foul_won += ",\'"+str(event['foul_won'][attr])+"\'"
                        else: 
                            insert_foul_won += ",\'false\'"
                else: 
                    insert_foul_won += ",\'false\',\'false\',\'false\'"
            
                insert_foul_won += "),"


            # # "Goal Keeper"
            elif type == "Goal Keeper":
                insert_goal_keeper += basic_insert[:-2]

                if 'goalkeeper' in event:
                    if 'location' in event['goalkeeper']:
                        insert_goal_keeper += ",\'"+event['goalkeeper']['location'][0]+"\',\'"+event['goalkeeper']['location'][1]+"\'"
                    else:
                        insert_goal_keeper += ",NULL,NULL"

                    name_attr = ['goal_position', 'outcome', 'type', 'body_part', 'technique']
                    for attr in name_attr:
                        if attr in event['goalkeeper']:
                            insert_goal_keeper += ",\'"+event['goalkeeper'][attr]['name']+"\'"
                        else: 
                            insert_goal_keeper += ",NULL"
                else: 
                    insert_goal_keeper += ",NULL,NULL,NULL,NULL,NULL"
            
                insert_goal_keeper += "),"
            
            # # "Interception"
            elif type == "Interception":
                insert_interception += basic_insert[:-2]+",\'"+event['interception']['outcome']['name']+"\'),"
            
            

        filenum += 1
        print("finished file" + str(filenum) + "/468")

    insert_ball_receipt = insert_ball_receipt[:-1] + ''
    cursor.execute(insert_ball_receipt)
    print('done insert ball receipt')
    insert_ball_recovery = insert_ball_recovery[:-1] + ''
    cursor.execute(insert_ball_recovery)
    print('done insert ball recovery')
    insert_block = insert_block[:-1] + ''
    cursor.execute(insert_block)
    print('done insert block')
    insert_dribbled_past = insert_dribbled_past[:-1] + ''
    cursor.execute(insert_dribbled_past)
    print('done insert dribbled past')

    # # FIX THIS PART
    insert_injury_stoppage = insert_injury_stoppage[:-1] + ''
    # print(insert_injury_stoppage) # --------------------------------!REMOVE!!!!!!!!!!!
    cursor.execute(insert_injury_stoppage)
    print('done insert injury stoppage')

    insert_5050 = insert_5050[:-1] + ''
    cursor.execute(insert_5050)
    print('done insert 5050')
    insert_bad_behaviour = insert_bad_behaviour[:-1] + ''
    cursor.execute(insert_bad_behaviour)
    print('done insert bad behaviour')
    insert_half_end = insert_half_end[:-1] + ''
    cursor.execute(insert_half_end)
    print('done insert half end')
    insert_half_start = insert_half_start[:-1] + ''
    cursor.execute(insert_half_start)
    print('done insert half start')
        
    insert_carry = insert_carry[:-1] + ''
    cursor.execute(insert_carry)
    print('done insert carry')
    insert_clearance = insert_clearance[:-1] + ''
    cursor.execute(insert_clearance)
    print('done insert clearance')
    insert_dribble = insert_dribble[:-1] + ''
    cursor.execute(insert_dribble)
    print('done insert dribble')
        
    # insert_duel = insert_duel[:-1] + ''
    # cursor.execute(insert_duel)
    # print('done insert duel')
    # insert_foul_committed = insert_foul_committed[:-1] + ''
    # cursor.execute(insert_foul_committed)
    # print('done insert foul_committed')
    # insert_duel = insert_duel[:-1] + ''
    # insert_foul_won = insert_foul_won[:-1] + ''
    # cursor.execute(insert_foul_won)
    # print('done insert foul_won')
    # insert_interception = insert_interception[:-1] + ''
    # cursor.execute(insert_interception)
    # print('done insert interinsert_interception')
        
    # insert_goal_keeper = insert_goal_keeper[:-1] + ''
    # cursor.execute(insert_goal_keeper)
    # print('done insert goal_keeper')




# delete_event_type_tables()
# create_event_type_tables()
# insert_events_type_data()


if __name__ == "__main__":
    conn.close()


