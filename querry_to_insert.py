from pymongo import MongoClient
try:
    client = MongoClient('localhost', 27017)
    print("Connected")
except:
    print("Print could not connect to MongoDB")

db = client['sample_mflix']
collection_comments = db['comments']
collection_movies = db['movies']
collection_theaters = db['theaters']
collection_users = db['users']

def insertComments(items):
    collection_comments.insert_one(items)

def insertMovies(items):
    collection_movies.insert_one(items)

def insertTheaters(items):
    collection_theaters.insert_one(items)

def insertUsers(items):
    collection_users.insert_one(items)