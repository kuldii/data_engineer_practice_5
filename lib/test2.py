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
    
    
# Output 5
output_age_group_by_city = []
for post in jobsC.aggregate([
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
    ]):
    output_age_group_by_city.append(post)
with open("assets/output/2/output_5_age_group_by_city.json", "w") as outfile:
    json.dump(output_age_group_by_city, outfile, indent=4, ensure_ascii=False)
    
# Output 6
output_age_group_by_job = []
for post in jobsC.aggregate([
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
    ]):
    output_age_group_by_job.append(post)
with open("assets/output/2/output_6_age_group_by_job.json", "w") as outfile:
    json.dump(output_age_group_by_job, outfile, indent=4, ensure_ascii=False)
    
# Output 7
output_max_salary_min_age = dict()
data_max_salary_min_age = []
for post in jobsC.aggregate([
        {
            "$group": {
                "_id" : "$age",
                "maxSalary": {
                    "$max": "$salary"
                }
            }
        }
    ]):
    data_max_salary_min_age.append(post)
data_max_salary_min_age = sorted(data_max_salary_min_age, reverse=False, key=lambda post: post["_id"] )
output_max_salary_min_age["minAge"] = data_max_salary_min_age[0]["_id"]
output_max_salary_min_age["maxSalary"] = data_max_salary_min_age[0]["maxSalary"]
with open("assets/output/2/output_7_max_salary_min_age.json", "w") as outfile:
    json.dump(output_max_salary_min_age, outfile, indent=4, ensure_ascii=False)
    
# Output 8
output_min_salary_max_age = dict()
data_min_salary_max_age = []
for post in jobsC.aggregate([
        {
            "$group": {
                "_id" : "$age",
                "minSalary": {
                    "$min": "$salary"
                }
            }
        }
    ]):
    data_min_salary_max_age.append(post)
data_min_salary_max_age = sorted(data_min_salary_max_age, reverse=True, key=lambda post: post["_id"] )
output_min_salary_max_age["maxAge"] = data_min_salary_max_age[0]["_id"]
output_min_salary_max_age["minSalary"] = data_min_salary_max_age[0]["minSalary"]
with open("assets/output/2/output_8_min_salary_max_age.json", "w") as outfile:
    json.dump(output_min_salary_max_age, outfile, indent=4, ensure_ascii=False)
