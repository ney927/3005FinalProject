import psycopg2
import json

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

# creates and fills the Competitions table
# IMPORTANT!! THIS WILL DELETE THE COMPETITIONS TABLE AFTER MAKING IT
# TODO -> THE SCHEMA FOR CREATE TABLE STATEMENT IS INCOMPLETED (missing stuff like unique, null, primary key, etc.)
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

# create competition table
cursor.execute('''CREATE TABLE IF NOT EXISTS Competitions(
               competition_id INT,
               season_id INT,
               country_name VARCHAR(255),
               competition_name VARCHAR(255),
               competition_gender VARCHAR(255),
               competition_youth BOOL,
               competition_international BOOL,
               season_name VARCHAR(255)
)''')

# insert competition data
cursor.execute(insert_competitions)

#print competition data
cursor.execute('SELECT * from Competitions')
rows = cursor.fetchall()
for row in rows:
    print(row)

cursor.execute('DROP TABLE IF EXISTS Competitions') #DELETES COMPETITION TABLE



conn.close()


'''
some bs

url = './opendata/data'
seasons = ['2003/2004' , '2018/2019', '2019/2020', '2020/2021']
relevant_matches = []


comp_id_season = []
competitions_json = json.load(open(url+'/competitions.json', 'r', encoding='utf-8'))
for comp in competitions_json:
    if comp['season_name'] in seasons:
        comp_id_season.append(str(comp['competition_id'])+'/'+str(comp['season_id']))

for comp in comp_id_season:
    match_json = json.load(open(url+'/matches/'+comp+'.json', 'r', encoding='utf-8'))
    relevant_matches += [str(match['match_id']) for match in match_json]

# for season in os.listdir(url+'/matches'):
#     for match in os.listdir(url+'/matches/'+season):
#         match_json = json.load(open(url+'/matches/'+season+'/'+match, 'r', encoding='utf-8'))
#         for match in match_json:
#             if match['season']['season_name'] in seasons:
#                 relevant_matches.append(match['match_id'])

print(len(relevant_matches), "relevent matches")
# print(relevant_matches)
cnt = 0
for match_id in os.listdir('./data/events'):
    if match_id[:-5] not in relevant_matches:
        cnt+=1
print(cnt)
'''