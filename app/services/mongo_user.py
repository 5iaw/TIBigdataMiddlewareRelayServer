from datetime import date, datetime
from pymongo import MongoClient
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv
from bson import ObjectId
load_dotenv()

MONGO_URI = f"mongodb://localhost:27017/"

# If your local MongoDB requires authentication, include username and password:
# MONGO_URI = f"mongodb://username:{escaped_password}@localhost:27017/"

client = MongoClient(MONGO_URI)
db = client['unik']
file_collection = db['files_folders']
users_collection = db['users']

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


def insert_user(username, email, role='user'):
    user_data = {
        "username": username,
        "email": email,
        "role": role,
        "hdfs_directory": f"/user/{username}/files",
        "created_at": datetime.now(),
        "last_login": None,
        "permissions": {
            "read": [f"/user/{username}/files"],
            "write": [f"/user/{username}/files"]
        },
        "metadata": {}
    }

    db.users.insert_one(user_data)
    return {"success": True, "message": f"Successfully added user {username}."}




def get_user(username):
    user = users_collection.find_one({"username": username})
    
    if user:
        user["_id"] = str(user["_id"])  # Convert ObjectId to string
        return {"success": True, "user": user}
    else:
        return {"success": False, "message": f"No user found with username {username}."}

def update_user(username, update_data):
    result = db.users.update_one({"username": username}, {"$set": update_data})
    
    if result.matched_count > 0:
        return {"success": True, "message": f"Successfully updated user {username}."}
    else:
        return {"success": False, "message": f"No user found with username {username}."}

def delete_user(username):
    result = db.users.delete_one({"username": username})
    
    if result.deleted_count > 0:
        return {"success": True, "message": f"Successfully deleted user {username}."}
    else:
        return {"success": False, "message": f"No user found with username {username}."}


def list_users(page=1, page_size=10):
    skip = (page - 1) * page_size
    results = users_collection.find().skip(skip).limit(page_size)
    users = list(results)
    
    return {"success": True, "users": users}
