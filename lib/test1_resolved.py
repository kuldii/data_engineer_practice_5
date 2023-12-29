from bson import ObjectId
import numpy as np
import pickle
import pymongo
import json

def inserData(coll, data, isMany=False):
    if(isMany == True):
        coll.insert_many(data)
    else:
        coll.insert_one(data)

def getDataPklFile(name, ext, varian):
    data = None
    with open(f"assets/data/var_{varian}/"+name+ext, 'rb') as pklFile:
        data = pickle.load(pklFile)
    return data

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
    with open("assets/output/1/resolved/"+name, 'w') as jsonFile:
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

fileName = "task_1_item"
varian = 12

db = connectDB()
jobsC = connectCollection(db,"jobs")

allData = getDataPklFile(fileName, ".pkl", varian)

if(jobsC.count_documents({}) == 0):
    inserData(jobsC, allData, isMany=True)
    
query_1 = [job for job in jobsC.find().sort("salary", pymongo.DESCENDING).limit(10)]
saveToJson("output_1.json", query_1)

query_2 = [job for job in jobsC.find({"age": {"$lt": 30}}).sort("salary", pymongo.DESCENDING).limit(15)]
saveToJson("output_2.json", query_2)

query_3 = [
    job for job in jobsC.find({
        "city": "Москва",
        "job": {"$in": ["Инженер","Менеджер","Учитель"]}
    }).sort("age", pymongo.ASCENDING).limit(10)
]
saveToJson("output_3.json", query_3)

query_4 = jobsC.count_documents({
    "age": {"$gte": 20, "$lte": 40},
    "year": {"$in": [2019, 2022]},
    "$or": [
        {"salary": {"$gt": 50000, "$lte": 75000}},
        {"salary": {"$gt": 125000, "$lt": 150000}}
    ]
})
query_4_json = {"count": query_4}
saveToJson("output_4.json", query_4_json)