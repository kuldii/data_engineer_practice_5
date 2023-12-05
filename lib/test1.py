import pickle
from pymongo import MongoClient
import pymongo
import json

fileName = "task_1_item"
varian = 12

def connectDB():
    client = MongoClient()
    return client["db-5"]

def readFile():
    allData = []
    with open(f"assets/data/var_{varian}/{fileName}.pkl", 'rb') as f:
        allDataFile = pickle.load(f)
        for dataFile in allDataFile:
            data = dict()
            for key in dataFile.keys():
                if(key == "id"):
                    data["_id"] = dataFile["id"]
                else:
                    data[key] = dataFile[key]
            allData.append(data)
    return allData

db = connectDB()
jobsC = db["jobs_1"]

if(jobsC.count_documents({}) == 0):
    allData = readFile()
    jobsC.insert_many(allData)

# Output 1
output_order_by_salary_desc = []
for post in jobsC.find().sort("salary", pymongo.DESCENDING).limit(10):
    output_order_by_salary_desc.append(post)
with open("assets/output/1/output_1_order_by_salary_desc.json", "w") as outfile:
    json.dump(output_order_by_salary_desc, outfile, indent=4, ensure_ascii=False)

# Output 2
output_filter_by_age_order_by_salary_desc = []
for post in jobsC.find({"age": {"$lt": 30}}).sort("salary", pymongo.DESCENDING).limit(15):
    output_filter_by_age_order_by_salary_desc.append(post)
with open("assets/output/1/output_2_filter_by_age_order_by_salary_desc.json", "w") as outfile:
    json.dump(output_filter_by_age_order_by_salary_desc, outfile, indent=4, ensure_ascii=False)

# Output 3
output_filter_complex_order_by_age_asc = []
for post in jobsC.find({"city": "Москва", "job": {"$in": ["Инженер","Менеджер","Учитель"]}}).sort("age", pymongo.ASCENDING).limit(10):
    output_filter_complex_order_by_age_asc.append(post)
with open("assets/output/1/output_3_filter_complex_order_by_age_asc.json", "w") as outfile:
    json.dump(output_filter_complex_order_by_age_asc, outfile, indent=4, ensure_ascii=False)

# Output 4
output_filter_range_complex = []
for post in jobsC.find(
    {
        "$and": [
            {
                "$and": [
                    {
                        "year": {"$gt": 2019},
                    },
                    {
                        "year": {"$lt": 2022},
                    }
                ]
            },
            {
                "$or": [
                    {
                        "$and": [
                            {
                                "salary": {"$gt": 50000},
                            },
                            {
                                "salary": {"$lte": 75000},
                            },
                        ]
                    },
                    {
                        "$and": [
                            {
                                "salary": {"$gt": 125000},
                            },
                            {
                                "salary": {"$lt": 150000},
                            },
                        ]
                    }
                ]
            },
        ]
    }
    ):
    output_filter_range_complex.append(post)
with open("assets/output/1/output_4_filter_range_complex.json", "w") as outfile:
    json.dump(output_filter_range_complex, outfile, indent=4, ensure_ascii=False)