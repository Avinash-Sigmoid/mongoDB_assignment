from pymongo import MongoClient
try:
    client = MongoClient('localhost', 27017)
    print("Connected")
except:
    print("Print could not connect to MongoDB")
client = MongoClient('localhost', 27017)

import json
from bson import ObjectId
db = client['sample_mflix']
mycol=db["movies"]
for x in mycol.find().limit(3):
    print(x)

collection_comments = db['comments']
comment_list=[]

with open('/Users/avinashkumar/Downloads/sample_mflix/comments.json') as f:
    for obj in f:
        if obj:
            my_dict = json.loads(obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            my_dict["date"] = my_dict["date"]["$date"]["$numberLong"]
            comment_list.append(my_dict)

collection_comments.insert_many(comment_list)


collection_movies = db['movies']
movies_list = []

with open('/Users/avinashkumar/Downloads/sample_mflix/movies.json') as f:
    for obj in f:
        if obj:
            my_dict = json.loads(obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            movies_list.append(my_dict)

collection_movies.insert_many(movies_list)

collection_theaters = db['theaters']
theater_list = []

with open('/Users/avinashkumar/Downloads/sample_mflix/theaters.json') as f:
    for obj in f:
        if obj:
            my_dict = json.loads(obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            my_dict["location"]["geo"]["coordinates"][0] = float(my_dict["location"]["geo"]["coordinates"][0]["$numberDouble"])
            my_dict["location"]["geo"]["coordinates"][1] = float(my_dict["location"]["geo"]["coordinates"][1]["$numberDouble"])
            theater_list.append(my_dict)

collection_theaters.insert_many(theater_list)

collection_users = db['users']
users_list = []

with open('/Users/avinashkumar/Downloads/sample_mflix/users.json') as f:
    for obj in f:
        if obj:
            my_dict = json.loads(obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            users_list.append(my_dict)

collection_users.insert_many(users_list)
