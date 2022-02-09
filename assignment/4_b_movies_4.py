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
        {
            '$unwind': {
                'path': '$genres',
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$project': {
                'title': 1,
                'rating': '$imdb.rating',
                'genres': 1
            }
        }, {
            '$sort': {
                'rating': -1
            }
        }, {
            '$match': {
                'rating': {
                    '$ne': ''
                }
            }
        }, {
            '$group': {
                '_id': '$genres',
                'movies': {
                    '$push': {
                        'movie': '$title',
                        'rating': '$rating'
                    }
                }
            }
        }, {
            '$project': {
                'movies': {
                    '$slice': [
                        '$movies', 0, n
                    ]
                }
            }
        }
    ])


for res in result:
    print(res)


client.close()