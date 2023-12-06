import msgpack
from pymongo import MongoClient
import pymongo
import json

fileName = "task_2_item"
varian = 12

def connectDB():
    client = MongoClient()
    return client["db-5"]

def readFile():
    allData = []
    with open("assets/data/var_12/"+fileName+".msgpack", "rb") as data_file:
        byte_data = data_file.read()
        data_loaded = msgpack.unpackb(byte_data)
        for dataFile in data_loaded:
            data = dict()
            for key in dataFile.keys():
                if(key == "id"):
                    data["_id"] = dataFile["id"]
                else:
                    data[key] = dataFile[key]
            allData.append(data)
    return allData

db = connectDB()
jobsC = db["jobs"]

if(jobsC.count_documents({}) == 0):
    allData = readFile()
    jobsC.insert_many(allData)
    
# Output 1
output_order_by_salary_desc = []
for post in jobsC.find().sort("salary", pymongo.DESCENDING).limit(10):
    output_order_by_salary_desc.append(post)
with open("assets/output/1/output_1_order_by_salary_desc.json", "w") as outfile:
    json.dump(output_order_by_salary_desc, outfile, indent=4, ensure_ascii=False)