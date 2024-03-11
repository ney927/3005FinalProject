import json
import os
url = './data'
seasons = ['2003/2004' , '2018/2019', '2019/2020', '2020/2021']
relevant_matches = []
# compeitions_json = json.load(open(url+'/competitions.json', 'r'))
# print(json.dumps(compeitions_json, indent=4))

for season in os.listdir(url+'/matches'):
    for match in os.listdir(url+'/matches/'+season):
        match_json = json.load(open(url+'/matches/'+season+'/'+match, 'r', encoding='utf-8'))
        for match in match_json:
            if match['season']['season_name'] in seasons:
                relevant_matches.append(match['match_id'])

print(len(relevant_matches), "relevent matches")

#not necessary anymore bc i removed all the irrelevant events from the folder
# relevant_events = []
# for event in os.listdir(url+'/events/'):
#     if int(event[:-5]) in relevant_matches:
#         relevant_events.append(event)
# print(len(relevant_events), "relevent events")

print(len(os.listdir(url+'/events/')), "relevant events")

#not necessary anymore ->  code that removed irrelevant lineups 
# for lineup in os.listdir(url+'/lineups/'):
#     if int(lineup[:-5]) not in relevant_matches:
#         os.remove(url+'/lineups/'+lineup)

print(len(os.listdir(url+'/lineups')), "relevant lineups")

#load all the relevent json files -> code needs to be updated b/c releant_events var doesn't exist
# for filename in relevant_events:
#     event_json = json.load(open(url+'/events/'+filename, 'r', encoding='utf-8'))
#     print(filename, "contains", len(event_json), "events")

# get compeitions.json file
# compeitions_json = json.load(open('./open-data/data/matches/11/1.json', 'r', encoding='utf-8'))
# print(json.dumps(compeitions_json, indent=4))

print('done')


# GET JSON FROM GITHUB
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