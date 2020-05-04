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
    client = MongoClient()
    user = auth_dict['username']
    password = auth_dict['password']
    authdb = auth_dict['authDB']
    db = auth_dict['db']

    client.csc369users.authenticate(user, password)



def main():
    auth_dict = parse_auth_file(sys.argv[1])
    print(auth_dict)
    connect_client(auth_dict)
main()

