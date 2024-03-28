import json
import os
url = './data' #this should be data b/c data doesn't include irrelevent events
seasons = ['2003/2004' , '2018/2019', '2019/2020', '2020/2021']
relevant_matches = []

# get compeitions.json file
# compeitions_json = json.load(open('./open-data/data/matches/11/1.json', 'r', encoding='utf-8'))
# print(json.dumps(compeitions_json, indent=4))

for season in os.listdir(url+'/matches'):
    for match in os.listdir(url+'/matches/'+season):
        match_json = json.load(open(url+'/matches/'+season+'/'+match, 'r', encoding='utf-8'))
        for match in match_json:
            if match['season']['season_name'] in seasons:
                relevant_matches.append(match['match_id'])

print(len(relevant_matches), "relevent matches")

print(len(os.listdir(url+'/events/')), "relevant events")

print(len(os.listdir(url+'/lineups')), "relevant lineups")

#load all the relevent json files -> this takes 20 seconds but doesn't loop through all events on all event files
for filename in os.listdir(url+'/events/'):
    event_json = json.load(open(url+'/events/'+filename, 'r', encoding='utf-8'))
    print(filename, "contains", len(event_json), "events")

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