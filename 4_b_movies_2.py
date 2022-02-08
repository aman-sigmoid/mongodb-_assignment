import pymongo
try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["mflix"]
except:
    print("Error in Connect")

movies=db["movies"]

n=10

# 1---------------------------------------------
#  who created the maximum number of movies

result= db.movies.aggregate([
    {"$project": {"directors": "$directors"}},
    {"$match": {"directors": {"$exists": "true", "$ne": "null"}}},
    {"$group": {"_id": {"directors": "$directors"}, "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": n}
])


for res in result:
    print(res)


# 2---------------------------------------------
#  who created the maximum number of movies in a given year

print('\n\n')
year=int(input("year : "))

result= db.movies.aggregate([
    {"$project": {"directors": "$directors", "year": "$year"}},
    {"$match": {"$and": [{"directors": {"$exists": "true", "$ne": "null"}}, {"year": year}]}},
    {"$group": {"_id": {"directors": "$directors"}, "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": n}
])

for res in result:
    print(res)

# 3---------------------------------------------
#  who created the maximum number of movies for a given genre
print('\n\n')
genres=input("genres : ")

result= db.movies.aggregate([
    {"$project": {"directors": "$directors", "genres": "$genres"}},
    {"$match": {"$and": [{"directors": {"$exists": "true", "$ne": "null"}}, {"genres": {"$in": [genres]}}]}},
    {"$group": {"_id": {"directors": "$directors"}, "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
])


for res in result:
    print(res)


client.close()