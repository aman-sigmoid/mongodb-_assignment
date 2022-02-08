import pymongo
try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["mflix"]
except:
    print("Error in Connect")

movies=db["movies"]

n=10

# ---------------------------------------------
# Find top `N` movies for each genre with the highest IMDB rating

result= db.movies.aggregate([
    {"$unwind": "$genres"},
    {"$project": {"rating": "$imdb.rating","genres": "$genres", "title": "$title"}},
    {"$group": {"_id": {"genres": "$genres", "max_rating": {"$max": "$rating"}, "title": {"first": "$title"}}}},
    {"$sort": {"_id.max_rating": -1}},
    {"$limit": n}
])


for res in result:
    print(res)


client.close()