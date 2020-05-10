# Shobit Asthana & Jeremy Whorton 
# COVID Tracker
# Lab 4 Mini Project

import sys
import string
import pymongo
import requests
import csv
import pandas as pd
import json
from pymongo import MongoClient
from datetime import datetime, timedelta
from bson.son import SON
import pprint

# Dictionary taken from https://gist.github.com/rogerallen/1583593
us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'American Samoa': 'AS',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Guam': 'GU',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands':'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}

def connect_client(auth_dict):
    if 'server' in auth_dict.keys():
        server = auth_dict['server'] + ":27017"
        print("server_verified")
    else:
        server = 'localhost:27017'
        
    user = auth_dict['username']
    if 'password' not in auth_dict.keys():
        password = input("Please enter your password: ")
    else:
        password = auth_dict['password']
    if password == '-1':
        password = input("Please enter your password: ")
    authdb = auth_dict['authDB']
    db = auth_dict['db']
    uri = 'mongodb://'+user+":"+password+"@"+server+"/"+authdb
    print(str(uri))
    uri = str(uri)
    #uri = 'mongodb://robot19:changeme@localhost:27017/csc369robots'
    client = MongoClient(uri)
    #client = MongoClient(server, username=user, password=password, authSource=authdb, authMechanism='SCRAM-SHA-1')
    print("database authenticated")
    return client

def load_daily():
    api_url = "https://covidtracking.com/api/v1/states/daily.json"
    resp = requests.get(api_url)
    if resp.status_code == 200:
        daily_json = json.loads(resp.content.decode('utf-8'))
        #daily_json = reformat_daily_dates(daily_json)
        return daily_json
    else:
        print("Failed to reach covid daily endpoint, please try again soon.")
        return None

def reformat_daily_dates(json):
    for doc in json:
        date = str(doc["date"])
        year = date[:4]
        month = date[4:6]
        day = date[6:8]
        reformatted_date = year[2:] + month + day
        doc["date"] = int(reformatted_date)
    return json

def state_code(state):
    return us_state_abbrev[state]

def load_states():
    api_url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv"
    response = requests.get(api_url)
    if response.status_code != 200:
        print("Failed to reach states endpoint, please try again soon")
        return None
    csv_resp = response.content.decode('utf-8')
    df = pd.DataFrame([x.split(',') for x in csv_resp.split('\n')])
    df.columns = df.iloc[0]
    df.drop(0, inplace=True)
    df["state"] = df["state"].apply(lambda s: state_code(s))
    df["date"] = df["date"].apply(lambda d: reformat_date_states(d))
    df["fips"] = pd.to_numeric(df["fips"])
    df["cases"] = pd.to_numeric(df["cases"])
    df["deaths"] = pd.to_numeric(df["deaths"])
    json = df.to_dict('records')
    return json

def reformat_date_states(date):
    year, month, day = date.split("-")
    date = year[2:] + month + day
    return int(date)

def update_collection(client, database, collection, json):
    db = client[database]
    collection = db[collection]
    collection.drop()
    result = collection.insert_many(json)

def parse_command_line(argv):
    auth_file = "credentials.json"
    config_file = "trackerConfig.json"
    for i in range(len(argv)):
        if argv[i] == "-auth":
            try:
                auth_file = argv[i+1]
            except IndexError as i:
                print("-auth flag must be followed by path to authentication file")
                return
        if argv[i] == "-config":
            try:
                config_file = argv[i+1]
            except IndexError as i:
                print("-config flag must be followed by path to configuration file")
                return
    return auth_file, config_file

def read_files(auth_file, config_file):
    try:
        auth_dict = parse_json_file(auth_file)
        config_dict = parse_json_file(config_file)
    except IndexError as i:
        print("Must specify database credentials")
        return
    except IOError as f:
        print("Unable to open file, please provide valid file path")
        print(f)
        return
    return auth_dict, config_dict

def main():
    auth_file, config_file = parse_command_line(sys.argv[1:])
    print("returned from command line parse")
    auth_dict, config_dict = read_files(auth_file, config_file)
    print("returned from reading files")
    mongo_client = connect_client(auth_dict)
    refresh_collection(auth_dict, config_dict, mongo_client)
    task_manager(auth_dict['db'], mongo_client, config_dict)

def refresh_collection(auth_dict, config_dict, mongo_client):
    json = None
    collection = config_dict["collection"]
    if config_dict["refresh"] == True:
        if collection == "covid":
            json = load_daily()
        if collection == "states":
            json = load_states()
    if json != None:
        update_collection(mongo_client, auth_dict['db'], collection, json)


def parse_json_file(filename):
    with open(filename) as json_file:
        data = json.load(json_file)
    print(data)
    return data

def pipeline_generator(config_dict):
    agg_pipeline = []
    if 'time' in config_dict.keys():
        time_pipe = create_time_query(config_dict)
        agg_pipeline.append(time_pipe)
    if 'target' in config_dict.keys():
        target_pipe = create_target_query(config_dict)
        agg_pipeline.append(target_pipe)
    if 'counties' in config_dict.keys():
        counties_pipe = create_counties_query(config_dict)
        agg_pipeline.append(counties_pipe)
    #result= db.command('aggregate','covid', pipeline= agg_pipeline, explain= True)
    #pprint.pprint(list(result))
    return agg_pipeline

def create_time_query(config_dict):
    time = config_dict['time']
    today = int(datetime.now().strftime("%Y%m%d"))
    yesterday = int((datetime.now() - timedelta(days = 1)).strftime("%Y%m%d"))
    #finds date exactly 1 week before today
    week = int((datetime.now() - timedelta(days = 7)).strftime("%Y%m%d"))
    month = int(str(today)[:6] + "01")
    time_dict = {'today': {"$match": {"date": today}}, 'yesterday': {"$match": {"date": yesterday}},
        'week': {"$match": {"date": {"$gte": week, "$lte": today}}}, 'month': {"$match": {"date": {"$gte": month,"$lte": today}}}}
    time_pipe = {}
    
    #creates time match pipe
    if not isinstance(time,dict):
        time_pipe = time_dict[config_dict['time']]
    else:
        #case when time gives start and end dates
        start = time['start']
        end = time['end']
        time_pipe = {"$match": {"date": {"$gte": start, "$lte": end}}}

    return time_pipe
def create_target_query(config_dict):
    #states collection will only contain a single state abbreviation
    target_pipe = {}
    state = config_dict['target']
    if config_dict['collection'] == 'states':
        
        target_pipe = {"$match": {"state": state}}
        return target_pipe
    else:
        # collection is covid
        # can be either single state abbreviation or list 
        if isinstance(state, list):
            target_pipe = {"$match": {"state": {"$in": state}}}
        else:
            target_pipe = {"$match": {"state": state}}
    return target_pipe

def create_counties_query(config_dict):
    #filter only applicable to states collection
    
    if config_dict['collection'] != 'states':
        return
    else:
        counties_pipe = {}
        counties = config_dict['counties']
        if isinstance(counties, list):
            counties_pipe = {"$match": {"county": {"$in": counties}}}
        else:
            counties_pipe = {"$match": {"county": counties}}
        return counties_pipe

def create_aggregation_query(config_dict, task):
    agg_level = config_dict["aggreation"]
    if agg_level == "usa":
        #no filter
        #group all observations into 1 result
        pass
    if agg_level == "fiftyStates":
        #filter to only 50 states + DC 
        #exclude 'American Samoa': 'AS','Guam': 'GU', 'Northern Mariana Islands':'MP', 'Puerto Rico': 'PR', 'Virgin Islands': 'VI'
        excluded_areas = ['AS', 'GU', 'MP', 'PR', 'VI']
        states_pipe = {"$match": {"state": {"$nin": excluded_areas}}}
    elif agg_level == "state":
        #filter will be hanled by state target query
        #group by state
        pass
    elif agg_level == "county":
        #filter will be handled by counties query
        #group by state, fips
        pass

def task_manager(database, client, config_dict):
    # approach: break down task field into number of pipelines and outputs
    num_tasks = len(config_dict['analysis'])
    db = client[database]
    collection = db[config_dict['collection']]
    '''
    similar to case/switch. these subfunctions will be used by calling task(job). For example
    take a task to be {'ratio': {'numerator': 'death', 'denominator': 'positive'}}}. Then when we call the function
    of the task's key name ie. ratio(task), it would call the ratio() function and would return the appropriate
    query. Things might get weird depending on aggregration. 
    '''
    
    def ratio(task):
        numerator = "$"+task['ratio']['numerator']
        denominator = "$"+task['ratio']['denominator']
        project_pipe = {"$project": {"date": 1, "ratio": {"$divide": [numerator,denominator]}}}
        return pipe
    def track(task):
        return
    def stats(task):
        return

    for job in config_dict['analysis']:
        pipeline = pipeline_generator(config_dict)
        print(pipeline)
        pprint.pprint(list(collection.aggregate(pipeline)))
        task = list(job['task'].keys())[0]


if __name__ == "__main__":
	main()

