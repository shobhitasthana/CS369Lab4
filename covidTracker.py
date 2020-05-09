# Shobit Asthana & Jeremy Whorton 
# COVID Tracker
# Lab 4 Mini Project

import sys
import string
import pymongo

from pymongo import MongoClient


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


def main():
	try:
		auth_dict = parse_auth_file(sys.argv[1])
	except IndexError as i:
		print("Must specify database credentials")
		return
	
	mongo_client = connect_client(auth_dict)
	load_data(auth_dict['db'], mongo_client)


if __name__ == "__main__":
	main()

