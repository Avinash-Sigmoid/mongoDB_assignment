from pymongo import MongoClient
try:
    client = MongoClient('localhost', 27017)
    print("Connected")
except:
    print("Print could not connect to MongoDB")