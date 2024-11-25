from pymongo import MongoClient
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import requests
from requests.auth import HTTPBasicAuth

load_dotenv()

# MongoDB setup
MONGO_URI = f"mongodb://localhost:27017/"

# If your local MongoDB requires authentication, include username and password:
# MONGO_URI = f"mongodb://username:{escaped_password}@localhost:27017/"

client = MongoClient(MONGO_URI)
db = client['unik']
file_collection = db['files_folders']

# WebHDFS setup
AUTH = HTTPBasicAuth('guest', 'guest-password')
WEBHDFS_URL = "https://203.252.112.33:8443/gateway/sandbox/webhdfs/v1/"

def webhdfs_request(path, op, method='GET', data=None, params=None, headers=None, verify=False):
    user_name = 'ubuntu'
    url = f"{WEBHDFS_URL}{path}?op={op}&user.name={user_name}"

    if params:
        url += '&' + '&'.join(f"{key}={value}" for key, value in params.items())

    if method == 'GET':
        response = requests.get(url, headers=headers, verify=verify, auth=AUTH)
    elif method == 'PUT':
        response = requests.put(url, headers=headers, data=data, verify=verify, auth=AUTH)
    elif method == 'DELETE':
        response = requests.delete(url, verify=verify, auth=AUTH)
    else:
        raise ValueError("Unsupported HTTP method")

    if response.status_code in range(200, 300):
        return response
    else:
        raise Exception(f"WebHDFS request failed with status {response.status_code}: {response.text}")

def create_folder_in_hdfs(path):
    try:
        webhdfs_request(path, 'MKDIRS', method='PUT', verify=False, auth=AUTH)
        print(f"Folder created at '{path}' in HDFS")
    except Exception as e:
        print(f"Failed to create folder in HDFS: {str(e)}")

def upload_file_to_hdfs(local_file_path, hdfs_dest_path):
    if os.path.exists(local_file_path):
        try:
            with open(local_file_path, 'rb') as file_data:
                webhdfs_request(hdfs_dest_path, 'CREATE', method='PUT', data=file_data, verify=False, auth=AUTH)
            print(f"File '{local_file_path}' uploaded to HDFS at '{hdfs_dest_path}'")
        except Exception as e:
            print(f"Failed to upload file to HDFS: {str(e)}")
    else:
        print(f"Local file '{local_file_path}' does not exist, skipping upload")

def delete_file_from_hdfs(hdfs_file_path):
    try:
        webhdfs_request(hdfs_file_path, 'DELETE', method='DELETE', verify=False, auth=AUTH)
        print(f"File '{hdfs_file_path}' deleted from HDFS")
    except Exception as e:
        print(f"Failed to delete file from HDFS: {str(e)}")

def move_file_in_hdfs(source_path, destination_path):
    try:
        webhdfs_request(source_path, 'RENAME', method='PUT', params={'destination': destination_path}, verify=False, auth=AUTH)
        print(f"File moved from '{source_path}' to '{destination_path}' in HDFS")
    except Exception as e:
        print(f"Failed to move file in HDFS: {str(e)}")

def insert_metadata_in_mongodb(metadata):
    result = file_collection.insert_one(metadata)
    if result.inserted_id:
        path = metadata.get('path')
        type_ = metadata.get('type')
        if type_ == 'folder':
            create_folder_in_hdfs(path)
        elif type_ == 'file':
            local_file_path = os.path.join(os.getcwd(), metadata.get('name'))
            upload_file_to_hdfs(local_file_path, path)

def delete_metadata_in_mongodb(path):
    result = file_collection.delete_one({"path": path})
    if result.deleted_count > 0:
        delete_file_from_hdfs(path)
        print(f"Metadata for '{path}' deleted from MongoDB and HDFS")
    else:
        print(f"No metadata found for '{path}' in MongoDB")

def update_metadata_in_mongodb(path, update_data):
    result = file_collection.update_one({"path": path}, {"$set": update_data})
    if result.matched_count > 0:
        if 'path' in update_data:
            new_path = update_data['path']
            move_file_in_hdfs(path, new_path)
            print(f"Metadata for '{path}' updated in MongoDB and moved in HDFS")
        else:
            print(f"Metadata for '{path}' updated in MongoDB")
    else:
        print(f"No metadata found for '{path}' in MongoDB")

# Example usage of MongoDB operations that reflect in HDFS
def example_usage():
    # Insert metadata example
    metadata = {
        "name": "example_file.txt",
        "path": "/example/example_file.txt",
        "type": "file",
        "owner": "user123",
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "size": 1024,
        "permissions": {"read": ["user123"], "write": ["user123"]}
    }
    insert_metadata_in_mongodb(metadata)

    # Delete metadata example
    delete_metadata_in_mongodb("/example/example_file.txt")

    # Update metadata example
    update_metadata_in_mongodb("/example/example_file.txt", {"path": "/new_example/example_file.txt"})

if __name__ == "__main__":
    example_usage()

