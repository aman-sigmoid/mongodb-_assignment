import pymongo
try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["mflix"]
except:
    print("Error in Connect")

theaters=db["theaters"]

n=10

# 1---------------------------------------------
# Top 10 cities with the maximum number of theatres

result= db.theaters.aggregate([
    {"$project": {"city": "$location.address.city","theater": "$theaterId"}},
    {"$group": {"_id": {"city": "$city", "theater": "$theater"}, "num": {"$sum": 1}}},
    {"$group": {"_id": "$_id.city", "theaterCount": {"$push": {"theaterName": "$_id.theater", "count": "$num"}}}},
    {"$project": {"_id": 1, "totalTheatersAtCity": { "$sum": "$theaterCount.count"}}},
    {"$sort": {"totalTheatersAtCity": -1}},
    {"$limit": n}
])

for res in result:
    print(res)


# 2---------------------------------------------
# top 10 theatres nearby given coordinates
print("\n\n")
result= db.theaters.aggregate([
    {
         "$geoNear":{
             "near": { "type": "Point", "coordinates": [-84.526169, 37.986019] },
             "maxDistance":10*10000000000000,
             "distanceField": "dist.calculated",
             "includeLocs": "dist.location",
             "distanceMultiplier":1/1000,
             "spherical": "true"
      }
     },
     {"$project": {"city": "$location.address.city", "distance": "$dist.calculated"}},
     {"$group": {"_id": {"distance": "$distance", "city" : "$city"} }},
     {"$sort": {"_id.distance": 1}},
     {"$limit": n}
    ]);

for res in result:
    print(res)



client.close()