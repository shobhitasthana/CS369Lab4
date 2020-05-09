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

def parse_auth_file(filename):
    with open(filename, 'r') as f:
        json_doc = f.read()
    res_dict = {}
    bad_chars = "\t\n \"\'{}"
    bare = json_doc.translate({ord(c): None for c in string.whitespace})
    bare = bare.replace("{", "").replace("}","")
    for key_pair in bare.split(','):
        key = key_pair.split(":")[0].replace("\"", "")
        value = key_pair.split(":")[1].replace("\"", "")
        res_dict[key] = value
    return res_dict

def connect_client(auth_dict):
    if 'server' in auth_dict.keys():
        server = auth_dict['server'] + ":27017"
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
    result = collection.insert_many(json)

def main():
    try:
        auth_dict = parse_auth_file(sys.argv[1])
        config_dict = parse_config_file(sys.argv[2])
    except IndexError as i:
        print("Must specify database credentials")
        return
    mongo_client = connect_client(auth_dict)
    daily_json = load_daily()
    state_json = load_states()
    reformat_date_states("2020-01-22")
    if daily_json != None:
        update_collection(mongo_client, auth_dict['db'], 'covid', daily_json)
    if state_json != None:
        update_collection(mongo_client, auth_dict['db'], 'states', state_json)
    query_generator(auth_dict['db'],mongo_client,config_dict)

def parse_config_file(filename):
    with open(filename) as json_file:
        data = json.load(json_file)
    print(data)
    #collection = data['collection']
    return data

def query_generator(database, client, config_dict):
    db = client[database]
    #whether to refresh collection
    if config_dict['refresh']:
        print("Call function to refresh collections")
    #get today's date formatted in YYYYMMDD
    today = int(datetime.now().strftime("%Y%m%d"))
    yesterday = int((datetime.now() - timedelta(days = 1)).strftime("%Y%m%d"))
    #finds date exactly 1 week before today
    week = int((datetime.now() - timedelta(days = 7)).strftime("%Y%m%d"))
    month = int(str(today)[:6] + "01")
    #print(month)
    #print(today,yesterday)
    #print(int(today))

    collection = db[config_dict['collection']]
    time = config_dict['time']
    agg_pipeline = []
    print(collection)
    time_dict = {'today': {"$match": {"date": today}}, 'yesterday': {"$match": {"date": yesterday}}, 
        'week': {"$match": {"date": {"$gte": week, "$lte": today}}}, 'month': {"$month": {"date": {"$gte": month,"$lte": today}}}}
    time_pipe = {}
    #creates time match pipe
    if not isinstance(time,dict):
        time_pipe = time_dict[config_dict['time']]
    else:
        start = time['start']
        end = time['end']
        time_pipe = {"$match": {"date": {"$gte": start, "$lte": end}}}
    
    print(time_pipe) 
    agg_pipeline.append(time_pipe)
    #result= db.command('aggregate','covid', pipeline= agg_pipeline, explain= True)
    #pprint.pprint(list(result))
    pprint.pprint(list(db.covid.aggregate(agg_pipeline)))



if __name__ == "__main__":
	main()

