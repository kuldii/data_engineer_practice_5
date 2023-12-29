from bson import ObjectId
import numpy as np
import pandas as pd
import pymongo
import json
import random

def updateData(coll, command, isMany=False, filter = {}):
    if(isMany == True):
        coll.update_many(filter, command)
    else:
        coll.update_one(filter, command)

def deleteData(coll, command, isMany=False):
    if(isMany == True):
        coll.delete_many(command)
    else:
        coll.delete_one(command)
        
def insertData(coll, data, isMany=False):
    if(isMany == True):
        coll.insert_many(data)
    else:
        coll.insert_one(data)

def getDataCsvFile(name, ext, varian):
    df = pd.read_csv(f"assets/data/var_{varian}/"+name+ext, delimiter=';')
    data_list = df.to_dict(orient='records')
    return data_list

def connectCollection(db, name):
    return db[name]

def connectDB():
    client = pymongo.MongoClient()
    return client["db-5"]

def default_json(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError("Type not serializable")

def saveToJson(name, data):
    with open("assets/output/3/resolved/"+name, 'w') as jsonFile:
        json.dump(data, jsonFile, indent=2, cls=NpEncoder, ensure_ascii=False, default=default_json)
        
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)
   
# ================================================= #

fileName = "task_3_item"
varian = 12

db = connectDB()
jobsC = connectCollection(db,"jobs")

allData = getDataCsvFile(fileName, ".csv", varian)

if(jobsC.count_documents({}) == 1185):
    insertData(jobsC, allData, isMany=True)

# Task 1    
command_1 = {
    '$or': [
        {'salary': {'$lt': 25000}}, 
        {'salary': {'$gt': 175000}}
    ]
}
deleteData(jobsC, command_1, isMany=True)

# Task 2
command_2 = {'$inc': {'age': 1}}
updateData(jobsC, command_2, isMany=True)

# Task 3
percent = 5
allJob =  [job for job in jobsC.aggregate([{"$group": {"_id": "$job"}}])]
randomJob = allJob[random.randint(0, len(allJob)-1)]["_id"]
filter = {'job': randomJob}
command_3 = {'$mul': {'salary': (1 + (percent / 100))}} 
updateData(jobsC, filter=filter, command=command_3, isMany=True)

# Task 4
percent = 7
allCity =  [city for city in jobsC.aggregate([{"$group": {"_id": "$city"}}])]
randomCity = allCity[random.randint(0, len(allCity)-1)]["_id"]
filter = {'city': randomCity}
command_4 = {'$mul': {'salary': (1 + (percent / 100))}} 
updateData(jobsC, filter=filter, command=command_4, isMany=True)

# Task 5
percent = 10
allCity =  [city for city in jobsC.aggregate([{"$group": {"_id": "$city"}}])]
allJob =  [job for job in jobsC.aggregate([{"$group": {"_id": "$job"}}])]
randomCity = allCity[random.randint(0, len(allCity)-1)]["_id"]
randomJob = allJob[random.randint(0, len(allJob)-1)]["_id"]
filter = {
    'city': randomCity,
    'job': randomJob,
    'age': {'$gte': random.randint(1, 10), '$lte': random.randint(11, 70)}
}
command_5 = {'$mul': {'salary': (1 + (percent / 100))}} 
updateData(jobsC, filter=filter, command=command_5, isMany=True)

# Task 6   
command_6 = {'age': {'$lt': 15}}
deleteData(jobsC, command_6, isMany=True)