from bson import ObjectId
import pandas as pd
import numpy as np
import pymongo
import json
import uuid
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

def connectCollection(db, name):
    return db[name]

def connectDB():
    client = pymongo.MongoClient()
    return client["market"]

def default_json(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError("Type not serializable")

def saveToJson(name, data):
    with open("assets/output/4/"+name, 'w') as jsonFile:
        json.dump(data, jsonFile, indent=2, cls=NpEncoder, ensure_ascii=False, default=default_json)

def getDataCsvFile(name, ext):
    df = pd.read_csv("assets/data/custom/"+name+ext)
    df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df.to_dict(orient='records')

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)
    
class Book:
    def __init__(self, title, writer, price, category, year, rating, id=None):
        self.id = id
        self.title = title
        self.writer = writer
        self.price = price
        self.category = category
        self.year = year
        self.rating = rating

    def to_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "writer": self.writer,
            "price": self.price,
            "category": self.category,
            "year": self.year,
            "rating": self.rating
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            data["title"],
            data["writer"],
            data["price"],
            data["category"],
            data["year"],
            data["rating"]
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls.from_dict(data)

class Customer:
    def __init__(self, name, address, age, sex, id=None):
        self.id = id
        self.name = name
        self.address = address
        self.age = age
        self.sex = sex

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "address": self.address,
            "age": self.age,
            "sex": self.sex
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            data["name"],
            data["address"],
            data["age"],
            data["sex"]
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls.from_dict(data)
 
class Transaction:
    def __init__(self, date, total, customer_id, book_id, id=None):
        self.id = id
        self.date = date
        self.total = total
        self.customer_id = customer_id
        self.book_id = book_id

    def to_dict(self):
        return {
            "id": self.id,
            "date": self.date,
            "total": self.total,
            "customer_id": self.customer_id,
            "book_id": self.book_id
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            data["date"],
            data["total"],
            data["customer_id"],
            data["book_id"]
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls.from_dict(data)

# ================================================= #

bookFileName = "books"
customerFileName = "customers"
transactionFileName = "transactions"
ext = ".csv"

allBooks = getDataCsvFile(bookFileName, ext)
allCustomers = getDataCsvFile(customerFileName, ext)

db = connectDB()

booksC = connectCollection(db,"books")
customersC = connectCollection(db,"customers")

if(booksC.count_documents({}) == 0 and customersC.count_documents({}) == 0):
    insertData(booksC, allBooks, isMany=True)
    insertData(customersC, allCustomers, isMany=True)
    
# выборка
# Task 1
query_1 = [book for book in booksC.find().sort("rating", pymongo.DESCENDING).limit(10)]
saveToJson("output_1_1.json", query_1)
# Task 2
query_2 = [book for book in booksC.find({"year": 2015}).sort("title", pymongo.ASCENDING)]
saveToJson("output_1_2.json", query_2)
# Task 3
query_3 = [book for book in booksC.find({"price": {"$gt": 25, "$lt": 27}}).sort("price", pymongo.ASCENDING)]
saveToJson("output_1_3.json", query_3)
# Task 4
query_4 = [customer for customer in customersC.find({"$and": [
    {"age": {"$gt": 50}},  
    {"sex": "male"}
]}).sort("age", pymongo.ASCENDING)]
saveToJson("output_1_4.json", query_4)
# Task 5
query_5 = [customer for customer in customersC.find({"$and": [
    {"age": {"$lt": 25}},  
    {"sex": "female"} 
]}).sort("age", pymongo.ASCENDING)]
saveToJson("output_1_5.json", query_5)

# выбора с агрегацией 
# Task 1
query_1 = [customer for customer in customersC.aggregate([
    {
        "$group": {
            "_id": "$sex",
            "total": {
                "$sum": 1
            }
        }
    }
])]
saveToJson("output_2_1.json", query_1)
# Task 2
query_2 = [customer for customer in customersC.aggregate([
    {
        "$group": {
            "_id": "$sex",
            "min_age": {
                "$min": "$age"
            }, 
            "avg_age": {
                "$avg": "$age"
            }, 
            "max_age": {
                "$max": "$age"
            }
        }
    }
])]
saveToJson("output_2_2.json", query_2)
# Task 3
query_3 = [book for book in booksC.aggregate([
    {"$match": {"year": {"$lt": 2000}}},
    {
        "$group": {
            "_id": "$category",
            "min_price": {
                "$min": "$price"
            }, 
            "avg_price": {
                "$avg": "$price"
            }, 
            "max_price": {
                "$max": "$price"
            }
        }
    }
])]
saveToJson("output_2_3.json", query_3)
# Task 4
query_4 = [book for book in booksC.aggregate([
    {
        "$group": {
            "_id": "$category",
            "total": {
                "$sum": 1
            }
        }
    }
])]
saveToJson("output_2_4.json", query_4)
# Task 5
query_5 = [book for book in booksC.aggregate([
    {"$match": {"year": {"$gt": 2010}}},
    {"$group": {"_id": "$category", "min_price": {"$min": "$price"}, "avg_price": {"$avg": "$price"}, "max_price": {"$max": "$price"}}},
])]
saveToJson("output_2_5.json", query_5)

# обновление/удаление данных
# Task 1
command_1 = {
    '$or': [
        {'age': {'$lt': 15}}, 
        {'age': {'$gt': 70}}
    ]
}
deleteData(customersC, command_1, isMany=True)
# Task 2
command_2 = {'$inc': {'age': 1}}
updateData(customersC, command_2, isMany=True)
# Task 3
percent = 5
allBooks =  [book for book in booksC.aggregate([{"$group": {"_id": "$category"}}])]
randomBooks = allBooks[random.randint(0, len(allBooks)-1)]["_id"]
filter = {'category': randomBooks}
command_3 = {'$mul': {'price': (1 + (percent / 100))}} 
updateData(booksC, filter=filter, command=command_3, isMany=True)
# Task 4   
command_4 = {'price': {'$lt': 15}}
deleteData(booksC, command_4, isMany=True)
# Task 5
command_5 = {'$inc': {'price': 1}}
updateData(booksC, command_5, isMany=True)