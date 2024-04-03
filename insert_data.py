import psycopg2
import json
import os
import re 

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
            grab_from_attribues = ["referee","stadium"]
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


                                # country id starts at position+18 
                                to_extract = value[position+17:position+90]
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

insert_country_data()
insert_referee_data()
insert_stadium_data()

conn.close()

print('done.')