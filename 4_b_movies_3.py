import pymongo
try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["mflix"]
except:
    print("Error in Connect")

movies=db["movies"]

n=10

# 1---------------------------------------------
# who starred in the maximum number of movies


result= db.movies.aggregate([
    {"$unwind": "$cast"},
    {"$project": {"actor": "$cast"}},
    {"$match": {"actor": {"$exists": "true", "$ne": "null"}}},
    {"$group": {"_id": {"actor": "$actor"}, "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": n}
])

for res in result:
    print(res)

# 2---------------------------------------------
# who starred in the maximum number of movies in a given year

print('\n\n')
year=int(input("year : "))


result= db.movies.aggregate([
    {"$unwind": "$cast"},
    {"$project": {"actor": "$cast", "year": "$year"}},
    {"$match": {"$and": [{"actor": {"$exists": "true", "$ne": "null"}}, {"year": year}]}},
    {"$group": {"_id": {"actor": "$actor"}, "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": n}
])


for res in result:
    print(res)

# 3---------------------------------------------
# who starred in the maximum number of movies for a given genre
print('\n\n')
genres=input("genres : ")


result= db.movies.aggregate([
    {"$unwind": "$cast"},
    {"$project": {"actor": "$cast", "year": "$year", "genres": "$genres"}},
    {"$match": {"$and": [{"actor": {"$exists": "true", "$ne": "null"}}, {"genres": {"$in": [genres]}}]}},
    {"$group": {"_id": {"actor": "$actor"}, "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": n}
])


for res in result:
    print(res)

client.close()