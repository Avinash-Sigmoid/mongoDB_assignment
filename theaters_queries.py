from pymongo import MongoClient
try:
    client = MongoClient('localhost', 27017)
    print("Connected")
except:
    print("Print could not connect to MongoDB")

db = client['sample_mflix']
collection_theaters = db['theaters']

#1. Top n cities with the maximum number of theaters
def city_with_max_no_theaters(n):
   city = collection_theaters.aggregate([{"$group":{"_id": "$location.address.city", "count": {"$sum": 1}}},
                               {"$project": {"location.address.city": 1, "count": 1}},
                                {"$sort": {"count": -1}},
                                {"$limit": n}])
   for x in city:
       print(x)

#city_with_max_no_theaters(5)

#2. top 10 theatres nearby given coordinates
def theaters_near_given_coordinates(lat, lng):
   output = collection_theaters.aggregate([{"$geoNear": {"near": {"type": "Point", "coordinates": [-91.24, 43.85]},
                               "maxDistance": 10000000, "distanceField": "distance"}},
                               {"$project": {"location.addess.city": 1, "_id": 0, "location.geo.coordinates": 1}},
                               {"$limit": 10}])
   for i in output:
       print(i)

theaters_near_given_coordinates(-91.24, 43.85)

