import pymongo
import json
from bson import ObjectId

try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["mflix"]
except:
    print("Error in Connect")

comments=db["comments"]
movies = db["movies"]
sessions=db["sessions"]
theaters=db["theaters"]
users=db["users"]
# -----------------------adding function to input data--------------------------

def add_comments(dic):
    return movies.insert_one(dic)

def add_movies(dic):
    return movies.insert_one(dic)

def add_sessions(dic):
    return movies.insert_one(dic)

def add_theaters(dic):
    return movies.insert_one(dic)

def add_users(dic):
    return movies.insert_one(dic)

#--------------------Loading Data from jason------------------------
data=[]
with open('/Users/amanverma/Documents/sample_mflix/comments.json') as f:
    for json_obj in f:
        if json_obj:
            my_dict = json.loads(json_obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            data.append(my_dict)

comments.insert_many(data)

with open('/Users/amanverma/Documents/sample_mflix/movies.json') as f:
    for json_obj in f:
        if json_obj:
            my_dict = json.loads(json_obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            data.append(my_dict)
movies.insert_many(data)

with open('/Users/amanverma/Documents/sample_mflix/sessions.json') as f:
    for json_obj in f:
        if json_obj:
            my_dict = json.loads(json_obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            data.append(my_dict)
sessions.insert_many(data)

with open('/Users/amanverma/Documents/sample_mflix/theaters.json.json') as f:
    for json_obj in f:
        if json_obj:
            my_dict = json.loads(json_obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            data.append(my_dict)
theaters.insert_many(data)

with open('/Users/amanverma/Documents/sample_mflix/users.json.json') as f:
    for json_obj in f:
        if json_obj:
            my_dict = json.loads(json_obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            data.append(my_dict)
users.insert_many(data)


client.close()