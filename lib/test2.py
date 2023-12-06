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

# 648 is a total documents in first task
# we need to add into the same collection
if(jobsC.count_documents({}) == 648):
    allData = readFile()
    jobsC.insert_many(allData)
    
# Output 1
data_order_by_salary_asc = []
totalSalary = 0
output_order_by_salary_asc = dict()

for post in jobsC.find().sort("salary", pymongo.ASCENDING):
    totalSalary += post["salary"]
    data_order_by_salary_asc.append(post)
    
output_order_by_salary_asc["avgSalary"] = totalSalary / len(data_order_by_salary_asc)
output_order_by_salary_asc["minSalary"] = data_order_by_salary_asc[0]["salary"]
output_order_by_salary_asc["maxSalary"] = data_order_by_salary_asc[-1]["salary"]

with open("assets/output/2/output_1_order_by_salary_asc.json", "w") as outfile:
    json.dump(output_order_by_salary_asc, outfile, indent=4, ensure_ascii=False)
    
# Output 2
output_group_by_job = []
for post in jobsC.aggregate([
        {
            "$group": {
                "_id" : "$job",
                "total":{
                    "$sum": 1
                }
            }
        }
    ]):
    output_group_by_job.append(post)
with open("assets/output/2/output_2_group_by_job.json", "w") as outfile:
    json.dump(output_group_by_job, outfile, indent=4, ensure_ascii=False)
    
# Output 3
output_group_by_city = []
for post in jobsC.aggregate([
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
    ]):
    output_group_by_city.append(post)
with open("assets/output/2/output_3_group_by_city.json", "w") as outfile:
    json.dump(output_group_by_city, outfile, indent=4, ensure_ascii=False)
    
# Output 4
output_group_by_job = []
for post in jobsC.aggregate([
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
    ]):
    output_group_by_job.append(post)
with open("assets/output/2/output_4_group_by_job.json", "w") as outfile:
    json.dump(output_group_by_job, outfile, indent=4, ensure_ascii=False)
    
    