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
        {'$geoNear': {'near': {'type': 'Point','coordinates': [-118.11414, 37.667957]},
                'maxDistance': 1000000,
                'distanceField': 'dist.calculated',
                'includeLocs': 'dist.location',
                'distanceMultiplier': 0.001,
                'spherical': True
            }
        },
    {'$project': {'theaterId': 1,'_id': 0,'city': '$location.address.city','distance': '$dist.calculated'}},
    {'$limit': n}
    ])

for res in result:
    print(f"City - {res['city']} ; TheaterId - {res['theaterId']} ; Distance - {res['distance']}")




client.close()