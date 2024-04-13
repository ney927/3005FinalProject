import psycopg2
import json
import os
import re 
import datetime
from neyha_relation_building import*

from db_secret import password
# from neyha_relation_building import*

#this file is to insert data into the made tables

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


def insert_country_data():
    #get all country data from teams, stadiums and referees

    directory = url + '/matches'
    #go through each match folder
    for foldername in os.listdir(directory):
        fol = os.path.join(directory, foldername)
        
        #go through each match file in the match folders
        for filename in os.listdir(fol):
            individual_match_json = json.load(open(fol + '/' + filename, 'r', encoding='utf-8'))

            #match_attributes = ["match_id", "match_date", "kick_off", "competition", "season", "home_team", "away_team", "home_score", "away_score", "match_status", "match_status_360",
            #                    "last_updated", "last_updated_360", "metadata", "match_week", "competition_stage", "stadium", "referee"]
            #"home_team", "away_team",
            grab_from_attribues = ["referee","stadium","away_team","home_team"]
            ct_atr = ["country"]

            #print('started loading country data from file: ' + filename)
            
            #for each match in a json file
            for match in individual_match_json:
                
                
                #look through dictionaries that we need
                for attribute in grab_from_attribues:
                    #verify attribute is present 
                    if attribute in match.keys():
                        #get country info from these dictionaries
                        for attr in ct_atr:
                                insertCount = '('
                                insert_country = 'INSERT INTO countrys (id, name) VALUES '
                                #if attr in attribute.asdict().keys():
                                value = str(match[attribute])
                                position = value.find('country')

                                position2 = value.find('manager')
                                to_extract2 = value[position2+90:position2+190]

                                #get additional countrys from managers, (ex comoros)
                                if to_extract2:
                                    pos_id = re.findall(r'\'id\': (.*?)\, \'name', to_extract2)
                                    pos_name = re.findall(r'\'name\': (.*?)\'}}', to_extract2)

                                    if pos_name and pos_id:  

                                        c_id = pos_id[0].replace('\'', '')
                                        c_name = pos_name[0].replace('\'', '')

                                        try_c = insert_country
                                        try_c +=  '('+ c_id + ',' + '\'' + c_name + '\'' + '),'

                                        try_c = try_c[:-1] + ' ON CONFLICT DO NOTHING'

                                        cursor.execute(try_c)



                                # country id starts at position+18 
                                to_extract = value[position+17:position+100]
                                #print(to_extract)
            
                                #grabing the country name
                                if (to_extract.find('\"') != -1) or to_extract.find('\'\'') != -1: 
                                    p = re.findall(r' \"(.*?)\"', to_extract)
                                    country_name = p[0]
                                    
                                else : 
                                    p = re.findall(r' \'(.*?)\'', to_extract)
                                    country_name = p[1]
                                country_id = to_extract.split(',')[0]
                                #print("-------------")
                                #print(p)

                                country_name = country_name.replace('\'', '\'\'')
                                #print(country_name + country_id)

                                insertCount +=  country_id + ',' + '\'' + country_name + '\'' + '),'
                                #print(insertCount)

                                insert_country += insertCount

                                #if country already in data skip
                                insert_country = insert_country[:-1] + ' ON CONFLICT DO NOTHING'

                                #print(insert_country)

                                cursor.execute(insert_country)
            #print('finished loading country data from file: ' + filename)

    #insert country data
    #cursor.execute(insert_competitions)

    print('done insert_country_data()')

def insert_country2_data():
    #get all country data from players

    directory = url + '/lineups'
    #go through each file in lineups folder
    for filename in os.listdir(directory):
        individual_match_json = json.load(open(directory + '/' + filename, 'r', encoding='utf-8'))

        grab_from_attribues = ["team_id","lineup"]

        #if filename != '7298.json':
        #    continue
            
        #print(foldername)
        #print(filename)
        #for each team in a json file
        for team in individual_match_json:

            #for each dictionary (match)
            for attribute in grab_from_attribues:
                #verify attribute is present
                 


                if attribute == "team_id":
                    team_id = str(team[attribute])
                    
                if attribute == "lineup":
                    lineup = str(team[attribute])
                    lineup = lineup.split('}]}')

                    #for each player
                    for val in lineup:
                        insert_country = 'INSERT INTO Countrys (id, name) VALUES ('

                        if "," not in val:
                            continue

                        country_info = val.split(",")

                        for country_data in country_info:
                            #print(player_data)

                            if "country" in country_data:
                                country_id = country_data.split(': ')[2]
                            
                            if "name" in country_data:
                                country_name = country_data.split('\': ')[1]
                                country_name = country_name[1:-1]
                                country_name = country_name.replace('}', '')
                                country_name = country_name.replace('\"', '')

                                if country_name.count('\'') == 1:
                                    country_name = country_name.replace('\'', '\'\'')



                        #print(player_id)
                        #print(player_name)
                        #print(player_nick)
                        #print(jersey_number)
                        #print(country_id)
                        #print(team_id)

                        insert_country += country_id + ',' + '\''+ country_name + '\'' + ') ON CONFLICT DO NOTHING'

                        cursor.execute(insert_country)

            
            #print('finished loading referee data from file: ' + filename)

    print('done insert_country2_data()')

def insert_referee_data():
    directory = url + '/matches'
    #go through each match folder
    for foldername in os.listdir(directory):
        fol = os.path.join(directory, foldername)
        
        #go through each match file in the match folders
        for filename in os.listdir(fol):
            individual_match_json = json.load(open(fol + '/' + filename, 'r', encoding='utf-8'))

            grab_from_attribues = ["referee"]
            
            #for each match in a json file
            for match in individual_match_json:
                
                
                #for each dictionary (match)
                for attribute in grab_from_attribues:
                    #verify attribute is present 
                    if attribute in match.keys():

                        #insertRef = '('
                        insert_referee = 'INSERT INTO Referees (id, name, country_id) VALUES ('

                        value = str(match[attribute])
                        value = value.split(',')

                        #get ref id
                        ref_id = value[0].split(' ')[1]
                        #print(ref_id)

                        #get ref name
                        ref_name = value[1].split(': ')[1]
                        ref_name = ref_name[1:-1]
                        #print(ref_name)

                        #get their country's id
                        country_id = value[2].split(': ')[2].replace(' ', '')
                        #print(country_id)

                        insert_referee += ref_id + ',' + '\'' + ref_name + '\'' + ','  + country_id + ') ON CONFLICT DO NOTHING'

                        cursor.execute(insert_referee)
            #print('finished loading referee data from file: ' + filename)

    print('done insert_referee_data()')

def insert_stadium_data():
    directory = url + '/matches'
    #go through each match folder
    for foldername in os.listdir(directory):
        fol = os.path.join(directory, foldername)
        
        #go through each match file in the match folders
        for filename in os.listdir(fol):
            individual_match_json = json.load(open(fol + '/' + filename, 'r', encoding='utf-8'))

            grab_from_attribues = ["stadium"]
            
            #for each match in a json file
            for match in individual_match_json:
                
                
                #for each dictionary (match)
                for attribute in grab_from_attribues:
                    #verify attribute is present 
                    if attribute in match.keys():

                        #insertRef = '('
                        insert_stadium = 'INSERT INTO Stadiums (id, name, country_id) VALUES ('

                        value = str(match[attribute])
                        value = value.split(',')

                        #get the stadium id
                        stadium_id = value[0].split(' ')[1]
                        #print(stadium_id)

                        #get stadium name
                        stadium_name = value[1].split(': ')[1]
                        stadium_name = stadium_name[1:-1]
                        stadium_name = stadium_name.replace('\'', '\'\'')
                        #print(stadium_name)

                        #get the country's id
                        country_id = value[2].split(': ')[2].replace(' ', '')
                        #print(country_id)

                        insert_stadium += stadium_id + ',' + '\'' + stadium_name + '\'' + ','  + country_id + ') ON CONFLICT DO NOTHING'

                        cursor.execute(insert_stadium)
            #print('finished loading referee data from file: ' + filename)

    print('done insert_stadium_data()')

def insert_team_data():
    directory = url + '/matches'
    #go through each match folder
    for foldername in os.listdir(directory):
        fol = os.path.join(directory, foldername)
        
        #go through each match file in the match folders
        for filename in os.listdir(fol):
            individual_match_json = json.load(open(fol + '/' + filename, 'r', encoding='utf-8'))

            grab_from_attribues = ["home_team","away_team"]
            
            #if filename != '75.json':
            #    continue
            #print(foldername)
            #print(filename)
            #for each match in a json file
            for match in individual_match_json:
                
                #for each dictionary (match)
                for attribute in grab_from_attribues:
                    #verify attribute is present 
                    if attribute in match.keys():

                        insert_team = 'INSERT INTO Teams (id, team_name, gender, team_group) VALUES ('

                        value = str(match[attribute])
                        #print(value)
                        value = value.split(',')

                        #get the team id
                        team_id = value[0].split(' ')[1]
                        #print(team_id)
                        #print(team_id)

                        #get team name
                        team_name = value[1].split(': ')[1]
                        team_name = team_name[1:-1]
                        team_name = team_name.replace('\'', '\'\'')
                        #print(team_name)

                        #get team gender 
                        team_gender = value[2].split(': ')[1]
                        team_gender = team_gender[1:-1]

                        #get team group 
                        team_group = value[3].split(': ')[1]
                        team_group = team_group.replace('\'', '')

                        #get the country's id
                        #country_id = value[2].split(': ')[2].replace(' ', '')
                        #print(country_id)

                        insert_team += team_id + ',' + '\'' + team_name + '\'' + ','  + '\'' + team_gender + '\'' + ',' + '\'' + team_group + '\'' + ') ON CONFLICT DO NOTHING'

                        cursor.execute(insert_team)
            #print('finished loading referee data from file: ' + filename)

    print('done insert_team_data()')

def insert_manager_data():
    directory = url + '/matches'
    #go through each match folder
    for foldername in os.listdir(directory):
        fol = os.path.join(directory, foldername)

        #go through each match file in the match folders
        for filename in os.listdir(fol):
            individual_match_json = json.load(open(fol + '/' + filename, 'r', encoding='utf-8'))

            grab_from_attribues = ["home_team","away_team"]
            ct_atr = ["manager"]
            
            #print(foldername)
            #print(filename)
            #for each match in a json file
            for match in individual_match_json:
                
                #for each dictionary (match)
                for attribute in grab_from_attribues:
                    #verify attribute is present 
                    if attribute in match.keys():
                    
                        for attr in ct_atr:

                            if ct_atr[0] in attr:
                                insert_manager = 'INSERT INTO Managers (id, name, nickname, date_of_birth, country_id, team_id) VALUES ('

                                value = str(match[attribute])
                            
                                #print(value)
                                #p = re.findall(r' \"(.*?)\"', to_extract)
                                team_id = re.findall(r'\_id(.*?)\'home_team_name', value)

                                #if its a home team manager
                                if team_id:
                                    team_id = re.findall(r'\: (.*?)\,', team_id[0])
                                    team_id = team_id[0]
                                else: 
                                    #away team manager
                                    team_id = re.findall(r'\_id(.*?)\'away_team_name', value)
                                    team_id = re.findall(r'\: (.*?)\,', team_id[0])
                                    team_id = team_id[0]

                                p = re.findall(r'\'managers\'(.*?)\}\}', value)

                                #print(p)
                                if p:
                                    value = p[0].split(',')

                                    #for o in value:
                                    #    print(o)

                                    #get the manager id
                                    manager_id = value[0].split('\': ')[1]
                                    #print(manager_id)

                                    #get manager name
                                    manager_name = value[1].split(': ')[1]
                                    manager_name = manager_name[1:-1]
                                    manager_name = manager_name.replace('\'', '\'\'')
                                    #print(manager_name)

                                    #get manager nickname
                                    manager_nick = value[2].split(': ')[1]
                                    manager_nick = manager_nick.replace('\'', '')
                                    #print(manager_nick)

                                    #get manager date of birth (dob)
                                    manager_dob = value[3].split(': ')[1]
                                    manager_dob = manager_dob.replace('\'', '')

                                    #get manager country id 
                                    manager_country_id = value[4].split(': ')[2]
                                    manager_country_id = manager_country_id.replace('\'', '')

                                    #if no dob found input null
                                    if 'None' in manager_dob:
                                        insert_manager += manager_id + ',' + '\'' + manager_name + '\'' + ','  + '\'' + manager_nick + '\'' + ',' + 'NULL' + ',' +  manager_country_id + ',' + team_id + ') ON CONFLICT DO NOTHING'
                                    else:
                                        year = manager_dob.split('-')[0]
                                        month = manager_dob.split('-')[1]
                                        day = manager_dob.split('-')[2]

                                        insert_manager += manager_id + ',' + '\'' + manager_name + '\'' + ','  + '\'' + manager_nick + '\'' + ',' + 'TO_DATE(\''+ year + month + day + '\',' + '\'YYYYMMDD\')'+ ',' +  manager_country_id + ',' + team_id + ') ON CONFLICT DO NOTHING'

                                    #print(manager_dob)

                                    cursor.execute(insert_manager)
            
            #print('finished loading referee data from file: ' + filename)

    print('done insert_manager_data()')

def insert_player_data():
    directory = url + '/lineups'
    #go through each file in lineups folder
    for filename in os.listdir(directory):
        individual_match_json = json.load(open(directory + '/' + filename, 'r', encoding='utf-8'))

        grab_from_attribues = ["team_id","lineup"]

        #if filename != '7298.json':
        #    continue
            
        #print(foldername)
        #print(filename)
        #for each team in a json file
        for team in individual_match_json:

            #for each dictionary (match)
            for attribute in grab_from_attribues:
                #verify attribute is present
                 


                if attribute == "team_id":
                    team_id = str(team[attribute])
                    
                if attribute == "lineup":
                    lineup = str(team[attribute])
                    lineup = lineup.split('}]}')

                    #for each player
                    for val in lineup:
                        insert_players = 'INSERT INTO Players (id, name, nickname, jersey_number, country_id, team_id) VALUES ('

                        if "," not in val:
                            continue

                        player_info = val.split(",")

                        for player_data in player_info:
                            #print(player_data)

                            if "player_id" in player_data:
                                player_id = player_data.split(': ')[1]
                            
                            if "player_name" in player_data:
                                player_name = player_data.split('\': ')[1]
                                player_name = player_name[1:-1]
                                #print(player_name)

                                player_name = player_name.replace('\'', '\'\'')


                            if "player_nickname" in player_data:
                                player_nick = player_data.split('\': ')[1]

                                if 'None' not in player_nick:
                                    player_nick = player_nick[1:-1]
                                    player_nick = player_nick.replace('\'', '\'\'')
                                    #print(player_nick)

                            if "jersey_number" in player_data:
                                jersey_number = player_data.split('\': ')[1]

                            if "country" in player_data:
                                country_id = player_data.split('id\': ')[1]

                        #print(player_id)
                        #print(player_name)
                        #print(player_nick)
                        #print(jersey_number)
                        #print(country_id)
                        #print(team_id)

                        if 'None' in player_nick:
                            insert_players += player_id + ',' + '\'' + player_name + '\'' + ',' + 'NULL' + ',' + jersey_number + ',' + country_id + ',' + team_id + ') ON CONFLICT DO NOTHING'
                        else:
                            insert_players += player_id + ',' + '\'' + player_name + '\'' + ',' + '\'' + player_nick + '\'' + ',' + jersey_number + ',' + country_id + ',' + team_id + ') ON CONFLICT DO NOTHING'

                        cursor.execute(insert_players)

            
            #print('finished loading referee data from file: ' + filename)

    print('done insert_player_data()')

def insert_events_data():
    directory = url + '/events'
    filenum = 0
    #go through each file in lineups folder

    
    for filename in os.listdir(directory):
        cursor.execute("SELECT season_id FROM matches WHERE matches.match_id="+filename[:-5]+";")
        season_id = str(cursor.fetchone()[0])

        individual_match_json = json.load(open(directory + '/' + filename, 'r', encoding='utf-8'))

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

                            insert_shot = 'INSERT INTO Shots(id, season_id, key_pass_id, end_location, areial_won, follows_dribble, first_time, open_goal, statsbomb_xg, deflected, technique, body_part, type, outcome,player_id) VALUES (' + e_id + ',' + season_id + ',' + key_pass_id + ',' +  shot_end_location + ',' +  areial_won + ',' +  follows_dribble + ',' +  first_time + ',' +  open_goal + ',' +  statsbomb_xg + ',' +  shot_deflected + ',' +  technique + ',' + body_part + ',' +  shot_type + ',' +  shot_outcome + ',' +  player_id + ') ON CONFLICT DO NOTHING'
                            list_of_insertions.append(insert_shot)

                            if "freeze_frame" in dict(event["shot"]):
                                freeze_array = list(dict(event["shot"])["freeze_frame"])

                                for freeze_elem in freeze_array:
                                    freeze_elem_dict = dict(freeze_elem)
                                    freeze_location = 'ARRAY' +  str(freeze_elem_dict["location"])
                                    freeze_player_id = str(dict(freeze_elem_dict["player"])["id"])
                                    freeze_player_position = '\'' + str(dict(freeze_elem_dict["position"])["name"]) + '\''
                                    freeze_teammate = str(freeze_elem_dict["teammate"])

                                    insert_freezie = 'INSERT INTO Freeze_Frame(event_id, location, player_id, position, teammate) VALUES (' + e_id + ',' + freeze_location + ',' + freeze_player_id + ',' + freeze_player_position + ',' + freeze_teammate + ') ON CONFLICT DO NOTHING'
                                    list_of_insertions.append(insert_freezie)



                                    


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

                            insert_pass = 'INSERT INTO Pass(id, season_id, assisted_shot_id, recipient_id, legnth, angle, height, end_location, backheel, deflected, miscommunication, cross_pass, cut_back, switch, shot_assist, goal_assist, body_part, type, outcome, technique, player_id) VALUES (' + e_id + ',' + season_id + ',' + assisted_shot_id + ',' +  recipient_id + ',' +  p_length + ',' +  p_angle + ',' +  p_height + ',' +  pass_end_location + ',' +  backheel + ',' +  pass_deflected + ',' +  miscommunication + ',' +  cross + ',' + cut_back + ',' +  switch + ',' +  shot_assist + ',' + goal_assist + ',' + pass_body_part + ',' + pass_type + ',' + p_outcome + ',' + p_technique + ',' + player_id + ') ON CONFLICT DO NOTHING'
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
            #print(e_id)
            #print(e_index)
            #print(e_period)
            #print(e_timestamp)
            #print(e_second)
            #print(e_minute)
            #print(e_type)
            #print(e_possession)
            #print(e_possession_team)
            #print(e_team)
            #print(e_duration)
            #print(e_tactics)

            insert_event +=  e_id  + ',' + match_id + ','+ e_index + ',' + e_period + ',' + e_timestamp + ',' +  e_minute + ',' + e_second + ',' +  e_type + ',' +  e_possession + ',' +  e_possession_team + ',' +  e_play_pattern + ',' +  e_team + ',' +  e_duration + ',' + e_tactics + ',' + location + ') ON CONFLICT DO NOTHING'
            
            list_of_insertions.insert(0,insert_event)

            for insertion in list_of_insertions:
                cursor.execute(insertion)
        filenum += 1
        print("finished file " + filename + ', ' + str(filenum) + "/468")

    print('done insert_events_data()')


def insert_all_data():
    insert_country_data()
    insert_country2_data()

    # #ney
    insert_competitions_data()
    
    insert_referee_data()
    
    insert_stadium_data()

    insert_team_data()

    insert_manager_data()

    insert_player_data()

    # #ney
    insert_matches_data()

    insert_events_data()

insert_all_data()
conn.close()

print('done.')