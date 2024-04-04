import psycopg2
import json
import os
import re 
import datetime

from db_secret import password

#this file is to insert data into the made tables

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

def insert_all_data():
    insert_country_data()
    insert_country2_data()
    
    insert_referee_data()
    
    insert_stadium_data()

    insert_team_data()

    insert_manager_data()

    insert_player_data()

insert_all_data()
conn.close()

print('done.')