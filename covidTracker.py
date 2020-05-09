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
	server = auth_dict['server']
	user = auth_dict['username']
	password = auth_dict['password']
	authdb = auth_dict['authDB']
	db = auth_dict['db']
	client = MongoClient(server, username=user, password=password, authSource=authdb)
	return client

def load_data(database, client):
	db = client[database]
	db.list_collection_names()



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

