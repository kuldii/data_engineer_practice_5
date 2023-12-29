from bson import ObjectId
import numpy as np
import msgpack
import pymongo
import json

def insertData(coll, data, isMany=False):
    if(isMany == True):
        coll.insert_many(data)
    else:
        coll.insert_one(data)

def getDataMsgpackFile(name, ext, varian):
    data = None
    with open(f"assets/data/var_{varian}/"+name+ext, 'rb') as msgpackFile:
        data = msgpack.unpack(msgpackFile, raw=False)
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
    with open("assets/output/2/resolved/"+name, 'w') as jsonFile:
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

fileName = "task_2_item"
varian = 12

db = connectDB()
jobsC = connectCollection(db,"jobs")

allData = getDataMsgpackFile(fileName, ".msgpack", varian)

if(jobsC.count_documents({}) == 648):
    inserData(jobsC, allData, isMany=True)
    
query_1 = [job for job in jobsC.aggregate([
    {
        "$group": {
            "_id": None,
            "min_salary": {
                "$min": "$salary"
            }, 
            "avg_salary": {
                "$avg": "$salary"
            }, 
            "max_salary": {
                "$max": "$salary"
            }
        }
    }
])]
saveToJson("output_1.json", query_1)
    
query_2 = [job for job in jobsC.aggregate([
    {
        "$group": {
            "_id": "$job", 
            "total": {
                "$sum": 1
            }
        }
    }
])]
saveToJson("output_2.json", query_2)
    
query_3 = [job for job in jobsC.aggregate([
    {
        "$group": {
            "_id" : "$city",
            "totalData":{
                "$sum": 1
            },
            "avgSalary": {
                "$avg": "$salary"
            },
            "minSalary": {
                "$min": "$salary"
            },
            "maxSalary": {
                "$max": "$salary"
            }
        }
    }
])]
saveToJson("output_3.json", query_3)
    
query_4 = [job for job in jobsC.aggregate([
    {
        "$group": {
            "_id" : "$job",
            "totalData":{
                "$sum": 1
            },
            "avgSalary": {
                "$avg": "$salary"
            },
            "minSalary": {
                "$min": "$salary"
            },
            "maxSalary": {
                "$max": "$salary"
            }
        }
    }
])]
saveToJson("output_4.json", query_4)
    
query_5 = [job for job in jobsC.aggregate([
    {
        "$group": {
            "_id" : "$city",
            "totalData":{
                "$sum": 1
            },
            "avgAge": {
                "$avg": "$age"
            },
            "minAge": {
                "$min": "$age"
            },
            "maxAge": {
                "$max": "$age"
            }
        }
    }
])]
saveToJson("output_5.json", query_5)
    
query_6 = [job for job in jobsC.aggregate([
    {
        "$group": {
            "_id" : "$job",
            "totalData":{
                "$sum": 1
            },
            "avgAge": {
                "$avg": "$age"
            },
            "minAge": {
                "$min": "$age"
            },
            "maxAge": {
                "$max": "$age"
            }
        }
    }
])]
saveToJson("output_6.json", query_6)
    
query_7 = [job for job in jobsC.aggregate([
        {"$group": {"_id": "$age", "max_salary": {"$max": "$salary"}}},
        {"$sort": {"_id": pymongo.ASCENDING}},
        {"$limit": 1}
])]
saveToJson("output_7.json", query_7)
    
query_8 = [job for job in jobsC.aggregate([
        {"$group": {"_id": "$age", "min_salary": {"$min": "$salary"}}},
        {"$sort": {"_id": pymongo.DESCENDING}},
        {"$limit": 1}
])]
saveToJson("output_8.json", query_8)
    
query_9 = [job for job in jobsC.aggregate([
        {"$match": {"salary": {"$gt": 50000}}},
        {"$group": {"_id": "$city", "min_age": {"$min": "$age"}, "avg_age": {"$avg": "$age"}, "max_age": {"$max": "$age"}}},
        {"$sort": {"min_age": pymongo.ASCENDING}}
])]
saveToJson("output_9.json", query_9)
    
query_10 = [job for job in jobsC.aggregate([
        {
            "$match": {
                "city": {"$in": ["Москва", "Мадрид"]},
                "job": {"$in": ["Водитель","Программист"]},
                "$or": [
                    {"age": {"$gt": 18, "$lt": 25}}, 
                    {"age": {"$gt": 50, "$lt": 65}}
                ]
            }
        },
        {
            "$group": {
                "_id": None, 
                "min_salary": {"$min": "$salary"}, 
                "avg_salary": {"$avg": "$salary"}, 
                "max_salary": {"$max": "$salary"}
            }
        }
])]
saveToJson("output_10.json", query_10)

query_11 = [job for job in jobsC.aggregate([
        {"$match": {"salary": {"$gt": 70000}}},
        {"$group": {"_id": "$job", "min_age": {"$min": "$age"}, "avg_age": {"$avg": "$age"}, "max_age": {"$max": "$age"}}},
        {"$sort": {"min_age": pymongo.ASCENDING}}
])]
saveToJson("output_11.json", query_11)