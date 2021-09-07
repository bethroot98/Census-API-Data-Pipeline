import requests
import json
import pandas as pd
import psycopg2 as ps

response_API = requests.get('https://api.census.gov/data/2019/acs/acs5?get=NAME,B01001_001E,B01001_002E,B01001_026E,B02001_002E,B02001_003E,B02001_004E,B02001_005E,B02001_006E,B02001_007E&for=block group:*&in=state:21&in=county:*&in=tract:*&key=<YOUR KEY GOES HERE>')
data = response_API.text
parse_json=json.loads(data)
for i in range(1, len(parse_json)):
    for j in range(1, len(parse_json[i])):
        if type(parse_json[i][j]) == str:
            parse_json[i][j] = int(parse_json[i][j])
    temp = parse_json[i][0].split(',')
    temp.reverse()
    parse_json[i].pop(0)
    index1 = 0
    for elem in temp:
        parse_json[i].insert(index1, elem.strip())
        index1 += 1
    index2 = 0
    for code in range(-len(temp),0):
        parse_json[i].insert(index2, parse_json[i].pop(code))
        index2 += 1
    
rows = parse_json[1:]
census_df = pd.DataFrame(rows, columns=['state_id', 'county_id', 'tract_id', 'block_group_id', 'state', 'county', 'tract', 'block_group', 
                                        'total_pop', 'male_pop', 'female_pop', 'white_pop', 'black_pop', 'native_american_pop', 
                                        'asian_pop', 'native_hawaiian_pop', 'other_race_alone_pop'])

response_API = requests.get('https://api.census.gov/data/2019/acs/acs5/subject?get=NAME,S1901_C01_013E,S2301_C04_001E&for=county:*&in=state:21&key=459e1b086f172f9f0e4e45c7291db3495c5906a7')

data = response_API.text

parse_json=json.loads(data)
for i in range(1, len(parse_json)):
    for j in range(1, len(parse_json[i])):
        if type(parse_json[i][j]) == str:
            parse_json[i][j] = float(parse_json[i][j])
    temp = parse_json[i][0].split(',')
    temp.reverse()
    parse_json[i].pop(0)
    index1 = 0
    for elem in temp:
        parse_json[i].insert(index1, elem.strip())
        index1 += 1
    index2 = 0
    for code in range(-len(temp),0):
        parse_json[i].insert(index2, parse_json[i].pop(code))
        index2 += 1

income_df = pd.DataFrame(parse_json[1:], columns=['state_id', 'county_id', 'state', 'county', 'avg_income', 'unemployment_rate'])

full_census_df = pd.merge(census_df, income_df, on=['state_id', 'county_id', 'state', 'county'])



def connect_to_db(host_name, dbname, port, username, password):
    try:
        conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
    except ps.OperationalError as e:
        raise e
    else:
        print('Connected!')
        return conn

host_name = input("Enter hostname:")
dbname = input("Enter database:")
uname = input("Enter username:")
pword = input("Enter password:")
port = input("Enter port:")

conn = connect_to_db(host_name, dbname, port, uname, pword)
curr = conn.cursor()
    
def create_table(curr, schemaname, tablename):
    create_table_command = ("""CREATE TABLE IF NOT EXISTS %s.%s (
        state_id INTEGER NOT NULL,
        county_id INTEGER NOT NULL,
        tract_id INTEGER NOT NULL,
        block_group_id INTEGER NOT NULL,
        state VARCHAR(100) NOT NULL,
        county VARCHAR(100) NOT NUlL, 
        tract VARCHAR(100) NOT NULL,
        block_group VARCHAR(100) NOT NULL,
        total_pop INTEGER NOT NULL,
        male_pop INTEGER NOT NULL,
        female_pop INTEGER NOT NULL,
        white_pop INTEGER NOT NULL,
        black_pop INTEGER NOT NULL,
        native_american_pop INTEGER NOT NULL,
        asian_pop INTEGER NOT NULL,
        native_hawaiian_pop INTEGER NOT NULL,
        other_race_alone_pop INTEGER NOT NULL,
        avg_income FLOAT NOT NULL,
        unemployment_rate FLOAT NOT NULL
        )""")
    
    curr.execute(create_table_command, [ps.extensions.AsIs(schemaname),ps.extensions.AsIs(tablename)])  
    
table_name = input("Enter table name:")
schema_name = input("Enter schema name:")
create_table(curr, schema_name, table_name)

conn.commit()

def insert_into_table(curr, state_id, county_id, tract_id, block_group_id, state, county, tract, block_group, total_pop, 
                      male_pop, female_pop, white_pop, black_pop, native_american_pop, asian_pop, native_hawaiian_pop, other_race_alone_pop,
                      avg_income, unemployment_rate):
    # Would want to change table name to correct table in order to insert data.
   insert_into_database = ("""INSERT INTO <YOUR TABLE NAME> (state_id, county_id, tract_id, block_group_id, state, county, tract, block_group, total_pop, 
                      male_pop, female_pop, white_pop, black_pop, native_american_pop, asian_pop, native_hawaiian_pop, other_race_alone_pop,
                      avg_income, unemployment_rate)
   VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""")
   row_to_insert = (state_id, county_id, tract_id, block_group_id, state, county, tract, block_group, total_pop, 
                      male_pop, female_pop, white_pop, black_pop, native_american_pop, asian_pop, native_hawaiian_pop, other_race_alone_pop,
                      avg_income, unemployment_rate)
   curr.execute(insert_into_database, row_to_insert)
   
def append_from_df_to_db(curr, df):
   for i, row in df.iterrows():
       insert_into_table(curr, row['state_id'], row['county_id'], row['tract_id'], row['block_group_id'], 
                         row['state'], row['county'], row['tract'], row['block_group'], 
                         row['total_pop'], row['male_pop'], row['female_pop'], 
                         row['white_pop'], row['black_pop'], row['native_american_pop'], row['asian_pop'], 
                         row['native_hawaiian_pop'], row['other_race_alone_pop'], row['avg_income'], row['unemployment_rate'])

append_from_df_to_db(curr, full_census_df)
conn.commit()
