# Shobit Asthana & Jeremy Whorton 
# COVID Tracker
# Lab 4 Mini Project

import sys
import string
import pymongo
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

def load_data(database, client):
        db = client.robot19
        db.list_collection_names()
        collection = db.test
        print("true")

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
    #db.command('aggregate',collection, pipeline= agg_pipeline)
    pprint.pprint(list(db.covid.aggregate(agg_pipeline)))
def main():
        try:
                auth_dict = parse_auth_file(sys.argv[1])
                config_dict = parse_config_file(sys.argv[2])
        except IndexError as i:
                print("Must specify database credentials")
                return

        mongo_client = connect_client(auth_dict)
        load_data(auth_dict['db'], mongo_client)
        query_generator(auth_dict['db'],mongo_client,config_dict)

if __name__ == "__main__":
	main()

