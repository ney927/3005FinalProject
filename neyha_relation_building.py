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
    cursor.execute("DROP TABLE IF EXISTS Goal_Keeper")
    
def create_event_type_tables():
    # "50/50", "Bad Behaviour", "Ball Receipt*", "Ball Recovery", "Block", "Carry", "Clearance", "Dribble", "Dribbled Past"
    # "Duel", "Foul Committed", "Foul Won", "Goal Keeper", "Half End", "Half Start", "Injury Stoppage", "Interception"
    cursor.execute('''CREATE TABLE IF NOT EXISTS event_5050(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   player_id INT NOT NULL,
                   position_name VARCHAR(255) NOT NULL,
                   season_id INT NOT NULL,
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
                   PRIMARY KEY (id),
                   FOREIGN KEY (id) references Events (id)
                        on delete set null
    )''')
    print("create Half_End table was successful!")
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Half_Start(
                   id VARCHAR(255) NOT NULL UNIQUE,
                   season_id INT NOT NULL,
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
    insert_ball_receipt = "INSERT INTO Ball_Receipt (id, player_id, position_name, season_id) VALUES "
    insert_ball_recovery = "INSERT INTO Ball_Recovery (id, player_id, position_name, season_id) VALUES "
    insert_block = "INSERT INTO block (id, player_id, position_name, season_id) VALUES "
    insert_injury_stoppage = "INSERT INTO injury_stoppage (id, player_id, position_name, season_id) VALUES "
    insert_5050 = "INSERT INTO event_5050 (id, player_id, position_name, season_id) VALUES "
    insert_bad_behaviour = "INSERT INTO bad_behaviour (id, player_id, position_name, season_id) VALUES "
    insert_dribbled_past = "INSERT INTO dribbled_past (id, player_id, position_name, season_id) VALUES "
    insert_half_end = "INSERT INTO half_end (id, season_id) VALUES "
    insert_half_start = "INSERT INTO half_start (id, season_id) VALUES "
    insert_carry = "INSERT INTO carry (id, player_id, position_name, season_id, under_pressure, end_location_x, end_location_y) VALUES "
    insert_clearance = "INSERT INTO clearance (id, player_id, position_name, season_id, under_pressure) VALUES "
    insert_dribble = "INSERT INTO dribble (id, player_id, position_name, season_id, nutmeg, outcome) VALUES "
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
                insert_ball_receipt += basic_insert
            # "Ball Recovery"
            if type == "Ball Recovery":  
                insert_ball_recovery += basic_insert
            # "Block"
            elif type == "Block":
                insert_block += basic_insert
            # "Dribbled Past"
            elif type == "Dribbled Past":
                insert_dribbled_past += basic_insert
            # "Injury Stoppage"
            elif type == "Injury Stoppage":
                insert_injury_stoppage += basic_insert
            # "50/50"
            elif type == "50/50":
                insert_5050 += basic_insert
            # "Bad Behaviour"
            elif type == "Bad Behaviour":
                insert_bad_behaviour += basic_insert
            # "Half End"
            elif type == "Half End":
                insert_half_end += basic_insert
            # "Half Start"
            elif type == "Half Start":
                insert_half_start += basic_insert

            # "Carry"
            if type == "Carry":
                if 'under_pressure' in event:
                    pressure = "\'"+str(event['under_pressure'])+"\'"
                else:
                    pressure = "NULL"
                insert_carry += basic_insert[:-2]+","+pressure+",\'"+str(event['carry']['end_location'][0])+"\',\'"+str(event['carry']['end_location'][1])+"\'),"
                
            # "Clearance"
            elif type == "Clearance":
                insert_clearance += basic_insert[:-2]+",\'"+str(event['under_pressure'])+"\'),"

            # "Dribble"
            elif type == "Dribble":
                if 'nutmeg' in event['dribble']:
                    insert_dribble += basic_insert[:-2]+",\'"+str(event['dribble']['nutmeg'])+"\',\'"+event['dribble']['outcome']['name']+"\'),"
                else:
                    insert_dribble += basic_insert[:-2]+",NULL,\'"+event['dribble']['outcome']['name']+"\'),"
            
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
    insert_ball_recovery = insert_ball_recovery[:-1] + ''
    cursor.execute(insert_ball_recovery)
    print('done insert ball recovery')
    insert_block = insert_block[:-1] + ''
    cursor.execute(insert_block)
    print('done insert block')
    insert_dribbled_past = insert_dribbled_past[:-1] + ''
    cursor.execute(insert_dribbled_past)
    print('done insert dribbled past')
    insert_injury_stoppage = insert_injury_stoppage[:-1] + ''
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
        
    insert_duel = insert_duel[:-1] + ''
    cursor.execute(insert_duel)
    print('done insert duel')
    insert_foul_committed = insert_foul_committed[:-1] + ''
    cursor.execute(insert_foul_committed)
    print('done insert foul_committed')
    insert_duel = insert_duel[:-1] + ''
    insert_foul_won = insert_foul_won[:-1] + ''
    cursor.execute(insert_foul_won)
    print('done insert foul_won')
    insert_interception = insert_interception[:-1] + ''
    cursor.execute(insert_interception)
    print('done insert interinsert_interception')
        
    insert_goal_keeper = insert_goal_keeper[:-1] + ''
    cursor.execute(insert_goal_keeper)
    print('done insert goal_keeper')



# def insert_events_data():
    directory = url + '/events'
    filenum = 0
    #go through each file in lineups folder
    for filename in os.listdir(directory):
        individual_match_json = json.load(open(directory + '/' + filename, 'r', encoding='utf-8'))

        grab_from_attribues = ["id","index","period","timestamp","minute","second","type","possession","possession_team","play_pattern","team","duration","tactics"]

        #if filename != '7298.json':
        #    continue

        #print(foldername)
        #print(filename)
        #for each team in a json file
        for event in individual_match_json:
            insert_event = 'INSERT INTO Events (id, index, period, timestamp, minute, second, type, possession, possesstion_team, play_pattern, team, duration, tactics) VALUES ('

            #for each dictionary (match)
            for attribute in grab_from_attribues:
                #verify attribute is present
                #if attribute in event:
                #    print(attribute + '===' + str(event[attribute]))

                # e shorthand for event
                if attribute == 'id':
                    e_id = str(event[attribute])

                if attribute == 'index':
                    e_index = str(event[attribute])

                if attribute == 'period':
                    e_period = str(event[attribute])

                if attribute == 'timestamp':
                    e_timestamp = str(event[attribute])

                if attribute == 'minute':
                    e_minute = str(event[attribute])

                if attribute == 'second':
                    e_second = str(event[attribute])

                if attribute == 'type':
                    e_type = str(event[attribute])
                    e_type = e_type.split(': ')[2]
                    e_type = e_type.replace('}', '')
                    e_type = e_type[1:-1]

                if attribute == 'possession':
                    e_possession = str(event[attribute])

                if attribute == 'possession_team':
                    e_possession_team = str(event[attribute])
                    e_possession_team = e_possession_team.split(',')[0].split(': ')[1]

                if attribute == 'play_pattern':
                    e_play_pattern = str(event[attribute])
                    e_play_pattern = e_play_pattern.split(',')[1].split(': ')[1]
                    e_play_pattern = e_play_pattern.replace('}', '')
                    e_play_pattern = e_play_pattern[1:-1]

                if attribute == 'team':
                    e_team = str(event[attribute])
                    e_team = e_team.split(',')[0].split(': ')[1]

                if attribute == 'duration':
                    if attribute in event:
                        e_duration = str(event[attribute])
                    else :
                        e_duration = 'NULL'


                if attribute == 'tactics':
                    if attribute in event:
                        e_tactics = str(event[attribute])
                        e_tactics = e_tactics.split('\'')[3]
                    else :
                        e_tactics = 'NULL'

            insert_event += '\'' + e_id + '\'' + ',' + e_index + ',' + e_period + ',' + '\'' + e_timestamp + '\'' + ',' +  e_second + ',' + e_minute + ',' + '\'' +  e_type + '\'' + ',' +  e_possession + ',' +  e_possession_team + ',' + '\'' + e_play_pattern + '\'' + ',' +  e_team + ',' +  e_duration + ',' + '\'' + e_tactics + '\'' + ') ON CONFLICT DO NOTHING'

            cursor.execute(insert_event)
        filenum += 1
        print("finished file" + str(filenum) + "/468")

    print('done insert_events_data()')



# def insert_q_events_data():
    directory = url + '/events'
    filenum = 0
    #go through each file in lineups folder
    for filename in os.listdir(directory):
        individual_match_json = json.load(open(directory + '/' + filename, 'r', encoding='utf-8'))

        #NEYHA ADDED THIS
        cursor.execute("SELECT season_id FROM matches WHERE matches.match_id="+filename[:-5]+";")
        season_id = str(cursor.fetchone()[0])

        grab_from_attribues = ["id","match_id","index","period","timestamp","minute","second","type","possession","possession_team","play_pattern","team","duration","tactics","location"]

        #if filename != '15946.json':
        #    continue
            
        #print(foldername)
        #print(filename)
        #for each event in a json file
        for event in individual_match_json:
            insert_event = 'INSERT INTO Events (id, match_id, index, period, timestamp, minute, second, type, possession, possesstion_team, play_pattern, team, duration, tactics, location) VALUES ('
            insert_miscontrol = ''
            list_of_insertions = []
            insert_all_shots = [] #NEYHA ADDED THIS
            #for each dictionary (match)
            for attribute in grab_from_attribues:
                #verify attribute is present
                #if attribute in event:
                #    print(attribute + '===' + str(event[attribute]))

                # e shorthand for event
                if attribute == 'id':
                    e_id = '\'' +  str(event[attribute]) + '\''

                if attribute == 'index':
                    e_index = str(event[attribute])

                if attribute == 'period':
                    e_period = str(event[attribute])

                if attribute == 'timestamp':
                    e_timestamp = '\'' + str(event[attribute]) + '\''

                if attribute == 'minute':
                    e_minute = str(event[attribute])

                if attribute == 'second':
                    e_second = str(event[attribute])

                if attribute == 'type':
                    e_type = '\'' +  str(dict(event[attribute])["name"])  + '\''

                    e_type_id = int(dict(event[attribute])["id"])
                    #print(e_type)
                    
                    #Misconntrol id = 38
                    if e_type_id == 38:
                        #print("misscontrolli")
                        if "miscontrol" in event:
                            aerial = str(dict(event["miscontrol"])["aerial_won"]) 
                            player_id =   str(dict(event["player"])["id"]) 
                            insert_miscontrol = 'INSERT INTO Miscontrol (id, areial_won, player_id) VALUES (' + e_id + ',' + aerial + ',' + player_id + ') ON CONFLICT DO NOTHING'
                            #cursor.execute(insert_miscontrol)
                            list_of_insertions.append(insert_miscontrol)


                        #print("ball receeeeeee")

                    #Player Off id = 27
                    if e_type_id == 27:
                        #print(filename)
                        if "player_off" in event:
                            permanent = str(dict(event["player_off"])["permanent"]) 
                            player_id =   str(dict(event["player"])["id"]) 
                            insert_player_off = 'INSERT INTO Player_Off (id, permanent, player_id) VALUES (' + e_id + ',' + permanent + ',' + player_id + ') ON CONFLICT DO NOTHING'
                            list_of_insertions.append(insert_player_off)
                    
                    #Pressure id = 17
                    if e_type_id == 17:
                        #print("1")
                        if "counterpress" in event:
                            #print("2")
                            counterpress = str(event["counterpress"]) 
                            player_id =   str(dict(event["player"])["id"]) 
                            insert_counterpress = 'INSERT INTO Player_Off (id, permanent, player_id) VALUES (' + e_id + ',' + counterpress + ',' + player_id + ') ON CONFLICT DO NOTHING'
                            list_of_insertions.append(insert_counterpress)

                    #Substitution id = 19
                    if e_type_id == 19:
                        if "substitution" in event:
                            #print("2")
                            replacement_id = str(dict(dict(event["substitution"])["replacement"])["id"]) 
                            sub_outcome = '\'' +  str(dict(dict(event["substitution"])["outcome"])["name"]) + '\''
                            player_id =   str(dict(event["player"])["id"]) 
                            insert_substitution = 'INSERT INTO Substitution(id, replacement_player_id, outcome, player_id) VALUES (' + e_id + ',' + replacement_id + ',' + sub_outcome + ',' + player_id + ') ON CONFLICT DO NOTHING'
                            list_of_insertions.append(insert_substitution)

                    #Shots id = 16 
                    if e_type_id == 16:
                        if "shot" in event:
                            if "key_pass_id" in dict(event["shot"]):
                                key_pass_id = '\''+ str((dict(event["shot"])["key_pass_id"])) + '\''
                            else:
                                 key_pass_id = 'NULL'
                            
                            shot_end_location = 'ARRAY' + str(dict(event["shot"])["end_location"])
                            if "areial_won" in dict(event["shot"]):
                                areial_won = str((dict(event["shot"])["areial_won"]))
                            else:
                                 areial_won = 'NULL'

                            if "follows_dribble" in dict(event["shot"]):
                                follows_dribble = str((dict(event["shot"])["follows_dribble"]))
                            else:
                                 follows_dribble = 'NULL'

                            if "first_time" in dict(event["shot"]):
                                first_time = str((dict(event["shot"])["first_time"]))
                            else:
                                first_time = 'NULL'

                            if "freeze_frame" in dict(event["shot"]):
                                #change
                                freeze_frame = 'NULL'
                                #'\"' + str((dict(event["shot"])["freeze_frame"])) + '\"'
                            else:
                                freeze_frame = 'NULL'

                            if "open_goal" in dict(event["shot"]):
                                open_goal = str((dict(event["shot"])["open_goal"]))
                            else:
                                open_goal = 'NULL'

                            statsbomb_xg = str((dict(event["shot"])["statsbomb_xg"])) 

                            if "deflected" in dict(event["shot"]):
                                shot_deflected = str((dict(event["shot"])["deflected"]))
                            else:
                                shot_deflected = 'NULL'

                            technique = '\'' + str(dict(dict(event["shot"])["technique"])["name"]) + '\''
                            
                            body_part = '\'' + str(dict(dict(event["shot"])["body_part"])["name"]) + '\''
                            
                            shot_type = '\'' + str(dict(dict(event["shot"])["type"])["name"]) + '\''

                            shot_outcome = '\'' + str(dict(dict(event["shot"])["outcome"])["name"]) + '\''

                            player_id =   str(dict(event["player"])["id"]) 

                            # NEYHA CHANGED THIS
                            insert_shot = 'INSERT INTO Shots(id, key_pass_id, end_location, areial_won, follows_dribble, first_time, freeze_frame, open_goal, statsbomb_xg, deflected, technique, body_part, type, outcome,player_id, season_id) VALUES (' + e_id + ',' + key_pass_id + ',' +  shot_end_location + ',' +  areial_won + ',' +  follows_dribble + ',' +  first_time + ',' +  freeze_frame + ',' +  open_goal + ',' +  statsbomb_xg + ',' +  shot_deflected + ',' +  technique + ',' + body_part + ',' +  shot_type + ',' +  shot_outcome + ',' +  player_id + ','+season_id+') ON CONFLICT DO NOTHING'
                            insert_all_shots.append(insert_shot)
                            # list_of_insertions.append(insert_shot)

                    #Pass id = 30
                    if e_type_id == 30:
                        if "pass" in event:
                            if "assisted_shot_id" in dict(event["pass"]):
                                assisted_shot_id = '\''+ str((dict(event["pass"])["assisted_shot_id"])) + '\''
                            else:
                                 assisted_shot_id = 'NULL'

                            if "recipient_id" in dict(event["pass"]):
                                recipient_id = str((dict(event["pass"])["recipient_id"]))
                            else:
                                 recipient_id = 'NULL'

                            p_length = str((dict(event["pass"])["length"])) 
                            p_angle = str((dict(event["pass"])["angle"]))
                            p_height = '\'' + str(dict(dict(event["pass"])["height"])["name"]) + '\''
                            pass_end_location = 'ARRAY' + str(dict(event["pass"])["end_location"])

                            if "backheel" in dict(event["pass"]):
                                backheel = str((dict(event["pass"])["backheel"]))
                            else:
                                 backheel = 'NULL'
                                
                            if "deflected" in dict(event["pass"]):
                                pass_deflected = str((dict(event["pass"])["deflected"]))
                            else:
                                pass_deflected = 'NULL'

                            if "miscommunication" in dict(event["pass"]):
                                miscommunication = str((dict(event["pass"])["miscommunication"]))
                            else:
                                miscommunication = 'NULL'

                            if "cross" in dict(event["pass"]):
                                cross = str((dict(event["pass"])["cross"]))
                            else:
                                cross = 'NULL'

                            if "cut_back" in dict(event["pass"]):
                                cut_back = str((dict(event["pass"])["cut_back"]))
                            else:
                                cut_back = 'NULL'

                            if "switch" in dict(event["pass"]):
                                switch = str((dict(event["pass"])["switch"]))
                            else:
                                switch = 'NULL'

                            if "shot_assist" in dict(event["pass"]):
                                shot_assist = str((dict(event["pass"])["shot_assist"]))
                            else:
                                shot_assist = 'NULL'

                            if "goal_assist" in dict(event["pass"]):
                                goal_assist = str((dict(event["pass"])["goal_assist"]))
                            else:
                                goal_assist = 'NULL'

                            if "body_part" in dict(event["pass"]):
                                pass_body_part = '\'' + str(dict(dict(event["pass"])["body_part"])["name"]) + '\''
                            else:
                                pass_body_part = 'NULL'

                            if "type" in dict(event["pass"]):
                                pass_type = '\'' + str(dict(dict(event["pass"])["type"])["name"]) + '\''
                            else:
                                pass_type = 'NULL'

                            if "outcome" in dict(event["pass"]):
                                p_outcome = '\'' + str(dict(dict(event["pass"])["outcome"])["name"]) + '\''
                            else:
                                p_outcome = 'NULL'

                            if "technique" in dict(event["pass"]):
                                p_technique = '\'' + str(dict(dict(event["pass"])["technique"])["name"]) + '\''
                            else:
                                p_technique = 'NULL'

                            player_id =   str(dict(event["player"])["id"]) 

                            insert_pass = 'INSERT INTO Pass(id, assisted_shot_id, recipient_id, legnth, angle, height, end_location, backheel, deflected, miscommunication, cross_pass, cut_back, switch, shot_assist, goal_assist, body_part, type, outcome, technique, player_id) VALUES (' + e_id + ',' + assisted_shot_id + ',' +  recipient_id + ',' +  p_length + ',' +  p_angle + ',' +  p_height + ',' +  pass_end_location + ',' +  backheel + ',' +  pass_deflected + ',' +  miscommunication + ',' +  cross + ',' + cut_back + ',' +  switch + ',' +  shot_assist + ',' + goal_assist + ',' + pass_body_part + ',' + pass_type + ',' + p_outcome + ',' + p_technique + ',' + player_id + ') ON CONFLICT DO NOTHING'
                            list_of_insertions.append(insert_pass)


                if attribute == 'possession':
                    e_possession = str(event[attribute])

                if attribute == 'possession_team':
                    e_possession_team = '\'' + str(dict(event[attribute])["id"]) + '\''

                if attribute == 'play_pattern':
                    e_play_pattern = '\'' + str(dict(event[attribute])["name"]) + '\''
                    

                if attribute == 'team':
                    e_team = str(event[attribute])
                    e_team = e_team.split(',')[0].split(': ')[1]

                if attribute == 'duration':
                    if attribute in event:
                        e_duration = str(event[attribute])
                    else :
                        e_duration = 'NULL'
                    

                if attribute == 'tactics':
                    if attribute in event and "name" in dict(event[attribute]):
                        e_tactics = '\'' + str(dict(event[attribute])["name"]) + '\''
                    else :
                        e_tactics = 'NULL'

                if attribute == 'location':
                    if attribute in event:
                        location = 'ARRAY' + str(event[attribute])
                    else :
                        location = 'NULL'

                if attribute == 'match_id':
                    match_id = str(filename).replace('.json', '')
            insert_event +=  e_id  + ',' + match_id + ','+ e_index + ',' + e_period + ',' + e_timestamp + ',' +  e_minute + ',' + e_second + ',' +  e_type + ',' +  e_possession + ',' +  e_possession_team + ',' +  e_play_pattern + ',' +  e_team + ',' +  e_duration + ',' + e_tactics + ',' + location + ') ON CONFLICT DO NOTHING'
            
            # list_of_insertions.insert(0,insert_event)

            # for insertion in list_of_insertions:
            #     cursor.execute(insertion)

            #NEYHA ADDED THIS
            for insertion in insert_all_shots:
                cursor.execute(insertion)
        filenum += 1
        print("finished file " + filename + ', ' + str(filenum) + "/468")

    print('done insert_events_data()')

if __name__ == "__main__":
    conn.close()

