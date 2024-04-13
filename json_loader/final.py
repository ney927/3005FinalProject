import json
import os
url = './data' 
seasons = ['2003/2004' , '2018/2019', '2019/2020', '2020/2021']
relevant_matches = []

#get all match_ids that are part of the seasons
# for season in os.listdir(url+'/matches'): 
#     for match in os.listdir(url+'/matches/'+season):
#         match_json = json.load(open(url+'/matches/'+season+'/'+match, 'r', encoding='utf-8'))
#         for match in match_json:
#             if match['season']['season_name'] in seasons:
#                 relevant_matches.append(match['match_id'])
# print(len(relevant_matches), "relevent matches")

print(len(os.listdir(url+'/events/')), "relevant events")

print(len(os.listdir(url+'/lineups')), "relevant lineups")

#load all the relevent json files -> this takes 20 seconds but doesn't loop through all events on all event files
# for filename in os.listdir(url+'/events/'):
#     event_json = json.load(open(url+'/events/'+filename, 'r', encoding='utf-8'))
#     print(filename, "contains", len(event_json), "events")


# get compeitions.json file
# generate the sql statement to insert competition data into the relation
competitions_json = json.load(open(url+'/competitions.json', 'r', encoding='utf-8'))
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






print('done')


# HOW TO GET JSON FROM GITHUB DIRECTLY
# import requests
# import json
# url = "https://raw.githubusercontent.com/statsbomb/open-data/0067cae166a56aa80b2ef18f61e16158d6a7359a/"
# # file = requests.get(url+"data/competitions.json")
# # competitions = file.json()

# file = requests.get("https://github.com/statsbomb/open-data/tree/0067cae166a56aa80b2ef18f61e16158d6a7359a/data/events")
# data = file.json()

# list = []
# for item in data['payload']['tree']['items']:
#   file = requests.get(url+item['path'])
#   print("got event for match "+item['path'])

# print('done')