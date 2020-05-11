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
import matplotlib.pyplot as plt

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

map_month = {
    '01':'January',
    '02':'February',
    '03':'March',
    '04':'April',
    '05':'May',
    '06':'June',
    '07':'July',
    '08':'August',
    '09':'September',
    '10':'October',
    '11':'November',
    '12':'December'
}

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
    uri = str(uri)
    #uri = 'mongodb://robot19:changeme@localhost:27017/csc369robots'
    client = MongoClient(uri)
    #client = MongoClient(server, username=user, password=password, authSource=authdb, authMechanism='SCRAM-SHA-1')
    return client

def load_daily():
    api_url = "https://covidtracking.com/api/v1/states/daily.json"
    resp = requests.get(api_url)
    if resp.status_code == 200:
        daily_json = json.loads(resp.content.decode('utf-8'))
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
    api_url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
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

def map_int_to_date(int):
    date_str = str(int)
    day = date_str[-2:]
    try:
        month = map_month[date_str[-4:-2]]
    except:
        return date_str
    year = date_str[:-4]
    if len(year) < 4:
        year = "20" + year
    return month + " " + day + ", " + year

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
    if config_dict["aggregation"] == 'fiftystates':
        excluded_areas = ['AS', 'GU', 'MP', 'PR', 'VI']
        target_pipe = {"$match": {"state": {"$nin": excluded_areas}}}
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

def create_aggregation_query(config_dict, field, task):
    agg_level = config_dict["aggregation"]
    if agg_level == "usa" or agg_level == "fiftyStates":
        if task == "track":
            pipeline = [{"$group": {"_id":"$date", field[0]: {"$sum": "$"+field[0]}}}, {"$sort": {"date":1}}, {"$project": {"date": "$_id","_id":0, field[0]: 1}}]
        if task == "ratio":
            pipeline = [{"$group": {"_id":"$date", field[0]: {"$sum": "$"+field[0]}, field[1]: {"$sum": "$"+field[1]}}}, {"$project": {"date": "$_id","_id":0, field[0]: 1, field[1]:1}}]
        if task == "stats":
            group_pipeline = {}
            for f in field:
                stat_pipe = {"avg"+f: {"$avg": "$"+f}, "std"+f: {"$stdDevPop": "$"+f}}
                group_pipeline.update(stat_pipe)
            pipeline = [{"$project": group_pipeline}]    
    elif agg_level == "state":
        if task == "track":
            if "counties" in config_dict.keys():
                pipeline = [{"$sort": {"date":1, "county":1}}]
            pipeline = [{"$sort": {"date":1, "state":1}}]
        if task == "ratio":
            pipeline = [{"$sort": {"date":1}}, {"$group": {"_id":"state","dateArray": {"$push":"$date"},"ratioArray": {"$push": "$ratio"}}}]
        if task == "stats":
            group_pipeline = {"_id":"$state"}
            for f in field:
                stat_pipe = {"avg"+f: {"$avg": "$"+f}, "std"+f: {"$stdDevPop": "$"+f}}
                group_pipeline.update(stat_pipe)
            pipeline = [{"$group": group_pipeline}, {"$sort": {"state":1}}]
    elif agg_level == "county":
        if task == "track":
            pipeline = [{"$sort": {"date":1}}]
        if task == "ratio":
            pipeline = [{"$sort": {"date":1}}, {"$group": {"_id":"county","dateArray": {"$push":"$date"},"ratioArray": {"$push": "$ratio"}}}]
        if task == "stats":
            group_pipeline = {"_id":"$county"}
            for f in field:
                stat_pipe = {"avg"+f: {"$avg": "$"+f}, "std"+f: {"$stdDevPop": "$"+f}}
                group_pipeline.update(stat_pipe)
            pipeline = [{"$group": group_pipeline}, {"$sort": {"county":1}}]
    return pipeline

def task_manager(database, client, config_dict):
    # approach: break down task field into number of pipelines and outputs
    # establish number of tasks
    num_tasks = len(config_dict['analysis'])
    
    # config database and collection
    db = client[database]
    collection = db[config_dict['collection']]

    # build generic pipeline based on filters
    pipeline = pipeline_generator(config_dict)
    '''
    similar to case/switch. these subfunctions will be used by calling task(job). For example
    take a task to be {'ratio': {'numerator': 'death', 'denominator': 'positive'}}}. Then when we call the function
    of the task's key name ie. ratio(task), it would call the ratio() function and would return the appropriate
    query. Things might get weird depending on aggregration. 
    '''
    
    def ratio(task):
        numerator = task['ratio']['numerator']
        denominator = task['ratio']['denominator']
        agg = create_aggregation_query(config_dict, [numerator, denominator], "ratio")
        pipe = [{"$project": {"_id": 0,"date": 1, "ratio": {"$divide": ["$"+numerator,"$"+denominator]}}}]
        return  pipe+ agg
    
    def track(task):
        field = task['track']
        if "counties" in config_dict.keys():
            return [{"$project": {"_id": 0, field: 1, "date": 1, "county":1}}]
        pipe = [{"$project": {"_id": 0, field: 1, "date": 1}}]
        sort = create_aggregation_query(config_dict, [field], "track")
        return pipe + sort

    def stats(task):
        # depends on aggregation level but would require a group operation with avg and std aggregate functions.
        fields = task["stats"]
        group = create_aggregation_query(config_dict, fields, "stats")
        return group

    task_dict = {'ratio': ratio, 'track': track, 'stats': stats}
    html = ""
    for job in config_dict['analysis']:
        task_name = list(job['task'].keys())[0]
        # call task subfunctions
        pipe = task_dict[task_name](job['task'])
        task = list(job['task'].keys())[0]
        query = pipeline + pipe
        data = list(collection.aggregate(query))
        df = pd.DataFrame(data)
        output_dict = job['output']
        if len(data) == 0:
            html += "<h6>No data to display.</h6>"
        else:
            if 'graph' in output_dict.keys():
                html = html + output_grapher(df, output_dict['graph'])
            if 'table' in output_dict.keys():
                html = html + output_table(df, output_dict['table'])
    
    return html

def output_grapher(data,output):
    graph_type = output['type']
    html = ""
    if 'legend' in output.keys():
        legend = True if output['legend'] == "on" else False
    else:
        legend = False
    combo = output['combo']
    if 'title' in output.keys():
        title = output['title']
    else:
        title = 'NoTitleGiven'
    if 'dateArray' in data.columns:
        date = data['dateArray'].iloc[0]
        ratio = data['ratioArray'].iloc[0]
        df_dict = {'date': date, 'ratio': ratio}
        df = pd.DataFrame(df_dict)
        df.plot(x = 'date', kind = graph_type, legend = legend, title = title)
        plt.savefig(title+'.png')
        html += "<img src=" + title + ".png>"
        return html
   # if graph_type == 'line':
    #    data.plot(x = 'date',kind = graph_type, legend = False, title = title)
    if combo == "seperate":
        if "state" in data.columns:
            unique_states = list(data["state"].unique())
            for state in unique_states:
                graph_df = data[data["state"] == state]
                graph_df.plot(kind = graph_type, legend = legend, title=title)
                plt.savefig(title + state + ".png")
                html += "<img src=" + title + state + ".png>"
        if "county" in data.columns:
            unique_counties = list(data["county"].unique())
            for county in unique_counties:
                graph_df = data[data["county"] == county]
                graph_df.plot(kind = graph_type, legend = legend, title=title)
                plt.savefig(title + county + ".png")
                html += "<img src=" + title + county + ".png>"
    elif combo == "split":
        if "state" in data.columns:
            data.plot(kind=graph_type, legend=legend, title=title, color="state") 
        if "county" in data.columns:
            data.plot(kind=graph_type, legend=legend, title=title, color="county")
    else:
        data.plot(kind=graph_type, legend=legend, title=title)
        plt.savefig(title + ".png")
        html += "<img src=" + title + ".png>"
    
    return html


def output_table(data, output):
    data.fillna(0, inplace=True)
    title = output["title"] if "title" in output.keys() else ""
    if 'dateArray' in data.columns:
        date = data['dateArray'].iloc[0]
        ratio = data['ratioArray'].iloc[0]
        df_dict = {'date': date, 'ratio': ratio}
        data = pd.DataFrame(df_dict)
    if "_id" in data.columns:
        if str(data.iloc[0]["_id"]).isnumeric():
            data.rename(columns={'_id':'date'}, inplace=True)
        else:
            data.rename(columns={'_id':'state'}, inplace=True)
    if "date" in data.columns:
        data["date"] = data["date"].apply(lambda d: map_int_to_date(d))
    html = generate_table_html(data, title)
    return html

def generate_table_html(df, table_title):
    title = "<h3>" + table_title + "</h3>"
    table = df.to_html(index=False)
    return title+table

def write_html(html, config, config_dict):
    if "output" in config_dict.keys():
        file_name = config_dict["output"]
    else:
        file_name = config.split(".")[0] + "out" + ".html"
    with open(file_name, 'w') as f:
        f.write(html)
    f.close()

def main():
    # parse command line for files
    auth_file, config_file = parse_command_line(sys.argv[1:])
    
    # read config and auth files
    auth_dict, config_dict = read_files(auth_file, config_file)
    
    # establish mongo connection
    mongo_client = connect_client(auth_dict)
    
    #refresh collections (if necessary)
    refresh_collection(auth_dict, config_dict, mongo_client)
    
    #perform all the tasks in the config doc
    out_html = task_manager(auth_dict['db'], mongo_client, config_dict)
    
    #write html to file
    write_html(out_html, config_file, config_dict)

if __name__ == "__main__":
	main()

