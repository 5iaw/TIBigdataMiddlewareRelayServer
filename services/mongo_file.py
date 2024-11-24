# mongo_file.py
from services.webhdfs import webDeleteFileFromHDFS
from pymongo import MongoClient
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

MONGO_URI = f"mongodb://localhost:27017/"

# If your local MongoDB requires authentication, include username and password:
# MONGO_URI = f"mongodb://username:{escaped_password}@localhost:27017/"

client = MongoClient(MONGO_URI)
db = client['unik']
file_collection = db['files_folders']

from bson import ObjectId  # Import ObjectId to handle MongoDB object IDs

def resolve_hdfs_paths(file_ids):
    # Convert each file_id string to an ObjectId instance
    object_ids = [ObjectId(file_id) for file_id in file_ids]
    
    query = {"_id": {"$in": object_ids}}
    projection = {"hdfs_file_path": 1}
    documents = file_collection.find(query, projection)
    
    hdfs_paths = [doc["hdfs_file_path"] for doc in documents if doc["hdfs_file_path"]]
    print("Resolved HDFS paths:", hdfs_paths)  # Log the resolved paths for debugging
    
    return hdfs_paths

def insert_file_folder(name, path, hdfs_path, owner, type, size=0, parent_path=None):
    metadata = {
        "type": type,
        "name": name,
        "path": path,
        "hdfs_file_path": hdfs_path,
        "owner": owner,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "size": size if type == 'file' else 0,
        "parent_path": parent_path
    }
    result = file_collection.insert_one(metadata)
    return {"success": True, "id": str(result.inserted_id), "message": f"Successfully inserted {name}."}

def delete_file_folder_by_id_with_hdfs(item_id):
    item = file_collection.find_one({"_id": item_id})
    if not item:
        return {"success": False, "message": f"No item found with id {item_id}."}

    # Check if it's a folder and delete its contents recursively
    if item["type"] == "folder":
        return delete_folder_recursively_with_hdfs(item["path"], item["hdfs_file_path"])
    else:
        # Delete a single file
        hdfs_path = item.get("hdfs_file_path")
        if hdfs_path:
            hdfs_delete_result = webDeleteFileFromHDFS(hdfs_path)
            if not hdfs_delete_result["success"]:
                return {"success": False, "message": hdfs_delete_result["message"]}
        file_collection.delete_one({"_id": item_id})
        return {"success": True, "message": f"Successfully deleted file with id {item_id}."}

def delete_folder_recursively_with_hdfs(folder_path, hdfs_folder_path):
    """
    Recursively delete a folder and all its contents from MongoDB.

    Args:
        folder_path (str): The path of the folder to delete.
    """
    # Find all items within this folder path, including the folder itself
    items_to_delete = file_collection.find({"path": {"$regex": f"^{folder_path}(/|$)"}})

    # Delete each file entry in MongoDB and HDFS if it's a file
    for item in items_to_delete:
        if item["type"] == "file":
            # Delete the file from HDFS
            hdfs_path = item.get("hdfs_file_path")
            if hdfs_path:
                webDeleteFileFromHDFS(hdfs_path)
                
        # Delete the entry from MongoDB
        file_collection.delete_one({"_id": item["_id"]})

    print(f"Deleted folder and its contents from MongoDB: {folder_path}")
    return {"success": True, "message": f"Successfully deleted folder {folder_path} and its contents from MongoDB"}

def create_folder(owner, parent_path, folder_name):
    path = f"{parent_path}/{folder_name}"
    return insert_file_folder(name=folder_name, path=path, hdfs_path="", owner=owner, type="folder", parent_path=parent_path)

def get_file_folder(path):
    result = file_collection.find_one({"path": path})
    if result:
        return {"success": True, "metadata": result}
    else:
        return {"success": False, "message": f"No file or folder found at {path}."}

def update_file_folder(path, update_data):
    result = file_collection.update_one({"path": path}, {"$set": update_data, "$currentDate": {"updated_at": True}})
    if result.matched_count > 0:
        return {"success": True, "message": f"Successfully updated metadata for {path}."}
    else:
        return {"success": False, "message": f"No file or folder found at {path}."}

def delete_file_folder(path):
    result = file_collection.delete_one({"path": path})
    if result.deleted_count > 0:
        return {"success": True, "message": f"Successfully deleted metadata for {path}."}
    else:
        return {"success": False, "message": f"No file or folder found at {path}."}

def list_folder_contents(owner, folder_path):
    # Query to filter files and folders based on the owner and parent_path
    query = {
        "owner": owner,
        "parent_path": folder_path
    }
    
    # Debug: Log the query being executed
    print(f"[DEBUG] Executing query: {query}")
    
    # MongoDB query execution
    results = file_collection.find(query)
    folder_contents = []

    # Process each document from the query results
    for item in results:
        item_dict = {
            "id": str(item["_id"]),  # Convert ObjectId to string for JSON compatibility
            "name": item["name"],
            "path": item["path"],
            "hdfs_file_path": item.get("hdfs_file_path"),
            "type": item["type"],
            "size": item.get("size", 0),  # Default size to 0 if missing
            "created_at": item["created_at"].isoformat() if item.get("created_at") else None,
            "updated_at": item["updated_at"].isoformat() if item.get("updated_at") else None,
            "is_analysis_result": item.get("is_analysis_result", False),  # Default to False if missing
            "analysis_result_type": item.get("analysis_result_type", None)  # Default to None if missing
        }
        # Append the processed item to the folder contents list
        folder_contents.append(item_dict)

    # Debug: Log the folder contents
    print(f"[DEBUG] Retrieved folder contents: {folder_contents}")

    # Return success or failure based on the folder contents
    return {"success": True, "contents": folder_contents} if folder_contents else {"success": False, "message": f"No contents found in {folder_path}."}

def move_file_or_folder(owner, current_path, new_path):
    item = file_collection.find_one({"path": current_path, "owner": owner})
    if not item:
        return {"success": False, "message": f"No item found at {current_path}."}

    if file_collection.find_one({"path": new_path}):
        return {"success": False, "message": f"An item already exists at {new_path}."}

    update_result = file_collection.update_one(
        {"path": current_path},
        {"$set": {"path": new_path, "updated_at": datetime.now(timezone.utc)}}
    )
    if update_result.matched_count > 0:
        return {"success": True, "message": f"Moved from {current_path} to {new_path}."}
    return {"success": False, "message": f"Failed to move {current_path}."}

def rename_file_or_folder(owner, path, new_name):
    item = file_collection.find_one({"path": path, "owner": owner})
    if not item:
        return {"success": False, "message": f"No item found at {path}."}

    new_path = f"{item['parent_path']}/{new_name}"
    if file_collection.find_one({"path": new_path}):
        return {"success": False, "message": f"An item already exists with the name {new_name}."}

    update_result = file_collection.update_one(
        {"path": path},
        {"$set": {"name": new_name, "path": new_path, "updated_at": datetime.now(timezone.utc)}}
    )
    if update_result.matched_count > 0:
        return {"success": True, "message": f"Renamed to {new_name}."}
    return {"success": False, "message": f"Failed to rename {path}."}

from bson import ObjectId  # Import ObjectId to handle MongoDB object IDs

def rename_file_or_folder_by_id(item_id, new_name):
    item = file_collection.find_one({"_id": ObjectId(item_id)})
    if not item:
        return {"success": False, "message": f"No item found with ID {item_id}."}

    old_path = item["path"]
    parent_path = item["parent_path"]
    new_path = f"{parent_path}/{new_name}"

    # Check if a file/folder with the new name already exists in the same parent path
    if file_collection.find_one({"path": new_path, "owner": item["owner"]}):
        return {"success": False, "message": f"An item already exists with the name {new_name} in the specified path."}

    # Update the target item's path and name
    file_collection.update_one(
        {"_id": ObjectId(item_id)},
        {"$set": {"name": new_name, "path": new_path, "updated_at": datetime.now(timezone.utc)}}
    )

    # If it's a folder, recursively update paths and parent paths of all nested items
    if item["type"] == "folder":
        old_base_path = f"{old_path}/"
        new_base_path = f"{new_path}/"

        # Find all nested items within the folder
        nested_items = file_collection.find({"path": {"$regex": f"^{old_base_path}"}})
        for nested_item in nested_items:
            # Update the `path` by replacing the old path prefix with the new one
            updated_path = nested_item["path"].replace(old_base_path, new_base_path, 1)

            # Set the `parent_path` to the new folder's path if it matches the old path
            updated_parent_path = new_path if nested_item["parent_path"] == old_path else nested_item["parent_path"]

            # Update the path and parent_path in MongoDB
            file_collection.update_one(
                {"_id": nested_item["_id"]},
                {
                    "$set": {
                        "path": updated_path,
                        "parent_path": updated_parent_path,
                        "updated_at": datetime.now(timezone.utc)
                    }
                }
            )

    return {"success": True, "message": f"Successfully renamed to {new_name} and updated paths and parent paths recursively."}

def move_file_or_folder_by_id(item_id, new_parent_path):
    # Retrieve the item by its ID
    item = file_collection.find_one({"_id": ObjectId(item_id)})
    if not item:
        return {"success": False, "message": f"No item found with ID {item_id}."}

    # Determine the new path based on the new parent path
    new_path = f"{new_parent_path}/{item['name']}"

    # Check if the destination path already has an item with the same name
    if file_collection.find_one({"path": new_path, "owner": item["owner"]}):
        return {"success": False, "message": f"An item already exists at {new_path}."}

    # If it's a file, simply update its path and parent_path
    if item["type"] == "file":
        file_collection.update_one(
            {"_id": ObjectId(item_id)},
            {
                "$set": {
                    "path": new_path,
                    "parent_path": new_parent_path,
                    "updated_at": datetime.now(timezone.utc),
                }
            }
        )
        return {"success": True, "message": f"File moved to {new_path}."}

    # If it's a folder, we need to update the folder and all its nested items
    elif item["type"] == "folder":
        old_base_path = f"{item['path']}/"
        new_base_path = f"{new_path}/"

        # Update the folder itself
        file_collection.update_one(
            {"_id": ObjectId(item_id)},
            {
                "$set": {
                    "path": new_path,
                    "parent_path": new_parent_path,
                    "updated_at": datetime.now(timezone.utc),
                }
            }
        )

        # Find all nested items within this folder
        nested_items = file_collection.find({"path": {"$regex": f"^{old_base_path}"}})
        for nested_item in nested_items:
            # Update the `path` by replacing the old path prefix with the new one
            updated_path = nested_item["path"].replace(old_base_path, new_base_path, 1)

            # Update `parent_path` to the new path if it's directly under the folder
            updated_parent_path = (
                new_path if nested_item["parent_path"] == item["path"] else nested_item["parent_path"].replace(old_base_path, new_base_path, 1)
            )

            # Update the path and parent_path in MongoDB
            file_collection.update_one(
                {"_id": nested_item["_id"]},
                {
                    "$set": {
                        "path": updated_path,
                        "parent_path": updated_parent_path,
                        "updated_at": datetime.now(timezone.utc),
                    }
                }
            )

        return {"success": True, "message": f"Folder moved to {new_path} with all nested items updated."}
