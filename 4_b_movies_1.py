import pymongo
import re
try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["mflix"]
except:
    print("Error in Connect")

movies=db["movies"]

n=10

# 1---------------------------------------------
# with the highest IMDB rating
result= db.movies.aggregate([
    {'$project': {'title': '$title','rating': '$imdb.rating'}},
    {'$match': {'rating': {'$exists': True,'$ne': ''}}},
    {'$group': {'_id': {'rating': '$rating','title': '$title'}}},
    {'$sort': {'_id.rating': -1}},
    {'$limit': n}
])
for res in result:
    print(res)
# 2---------------------------------------------
# with the highest IMDB rating in a given year

print('\n\n')
year=int(input("year : "))

result= db.movies.aggregate([
    {"$project": {"year": {"$year": "$released"}, "rating": "$imdb.rating", "title": "$title"}},
    {"$match": {"year": year,'rating': {'$exists': True,'$ne': ''}}},
    {"$group": {"_id": {"title": "$title", "rating": "$rating"}}},
    {"$sort": {"_id.rating": -1}},
    {"$limit": n}
])

for res in result:
    print(res)

# 3------------------------------------------------
# with highest IMDB rating with number of votes > 1000

print('\n\n')
year=int(input("year : "))

result= db.movies.aggregate([
    {"$project": {"votes": "$imdb.votes", "rating": "$imdb.rating", "title": "$title"}},
    {"$match": {"votes": {"$gt": 1000}}},
    {"$group": {"_id": {"title": "$title", "rating": "$rating", "votes": "$votes"}}},
    {"$sort": {"_id.rating": -1, "_id.votes": -1}},
    {"$limit": n}
])


for res in result:
    print(res)


# 4------------------------------------------------
# with title matching a given pattern sorted by highest tomatoes ratings

print('\n\n')

regx = re.compile("^b", re.IGNORECASE)

result= db.movies.aggregate([
    {"$project": {"title": "$title", "rating": "$tomatoes.viewer.rating"}},
    {"$match": {"title": regx}},
    {"$group": {"_id": {"rating": "$rating", "title": "$title" }}},
    {"$sort": {"_id.rating": -1}},
    {"$limit": n}
])


for res in result:
    print(res)




client.close()