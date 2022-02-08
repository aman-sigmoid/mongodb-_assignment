import pymongo

try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["mflix"]
except:
    print("Error in Connect")

comments=db["comments"]
movies=db["movies"]
# ---------------------------------------------
#  Find top 10 users who made the maximum number of comments

result=db.comments.aggregate([
    { '$group': { '_id': { 'name': '$name' }, 'total_comments': { '$sum': 1 } } },
    { '$sort': { 'total_comments': -1 } },
    { '$limit': 10}
])

for res in result:
    print(res)


print("\n\n")
#-------------------------------------------
# Find top 10 movies with most comments
result =db.comments.aggregate([
    { '$group': { '_id': { 'movies': "$movie_id" }, 'totalcomments': { '$sum': 1 } } },
    { '$sort': { 'totalcomments': -1 } },
    { '$limit': 10}
])

for res in result:
    print(res)
print("\n\n")
#---------------------------------------------------------
# Given a year find the total number of comments created each month in that year
year=int(input("year : "))
result=db.comments.aggregate([
    { '$project': {'month':{'$month':'$date'}, 'year':{'$year' : '$date' } } },
    { '$match': {'year': year}},
    { '$group': { '_id': '$month', 'totalcomments': { '$sum': 1 } } },
    { '$sort' : { '_id': 1}}
])

for res in result:
    print(res)



client.close()