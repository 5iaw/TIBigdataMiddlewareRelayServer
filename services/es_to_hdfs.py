import os
from datetime import datetime

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from flask import Flask, json, jsonify, request
from hdfs import InsecureClient
from services.mongo_file import insert_file_folder
from utils.es_utils import getESQueryByID

client = InsecureClient('http://localhost:9870', user='ubuntu')
INDEX = "kubic_paper,kubic_news"

# Elasticsearch connection
es = Elasticsearch(
    [os.getenv('ELASTIC_HOST')],
    http_auth=(os.getenv('ELASTIC_ID'), os.getenv('ELASTIC_PASSWORD')),
    scheme="https",
    port=os.getenv('ELASTIC_PORT'),
    verify_certs=False  # Disable SSL certificate verification
)
def transfer_es_data_to_hdfs(es_id: str, hdfs_dest_path: str) -> dict:

    try:
        from middleware import getESQueryByID
        es_data = getESQueryByID(es_id)

        json_data = json.dumps(es_data, indent=4, ensure_ascii=False)

        local_file_path = f"./tmp/es_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        with open(local_file_path, 'w', encoding='utf-8') as json_file:
            json_file.write(json_data)

        with open(local_file_path, 'rb') as file_data:
            client.write(hdfs_dest_path, file_data, overwrite=True)

        os.remove(local_file_path)

        return {
            "success": True,
            "message": f"Successfully transferred data from Elasticsearch to HDFS at {hdfs_dest_path}",
            "hdfs_path": hdfs_dest_path
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to transfer data to HDFS: {str(e)}"
        }


def transfer_es_data_to_hdfs_with_mongo(es_id: str, hdfs_dest_path: str, owner: str) -> dict:

    try:
        # Fetch data from Elasticsearch
        es_data = getESQueryByID(es_id)
        json_data = json.dumps(es_data, indent=4, ensure_ascii=False)
        
        # Create a temporary local file
        local_file_path = f"./tmp/es_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        with open(local_file_path, 'w', encoding='utf-8') as json_file:
            json_file.write(json_data)
        
        # Write the file to HDFS
        with open(local_file_path, 'rb') as file_data:
            client.write(hdfs_dest_path, file_data, overwrite=True)
        
        # Get file size for metadata
        file_size = os.path.getsize(local_file_path)
        os.remove(local_file_path)  # Clean up the local file

        # Insert file metadata into MongoDB
        mongo_result = insert_file_folder(
            name=f"es_data_{es_id}.json",
            path=f"/users/{owner}/personal_files/es_data_{es_id}.json",
            hdfs_path=hdfs_dest_path,
            owner=owner,
            type="file",
            size=file_size
        )

        if not mongo_result['success']:
            return {
                "success": False,
                "message": "Transfer to HDFS succeeded, but failed to insert metadata into MongoDB."
            }

        return {
            "success": True,
            "message": f"Successfully transferred data from Elasticsearch to HDFS at {hdfs_dest_path} and reflected in MongoDB.",
            "hdfs_path": hdfs_dest_path,
            "mongo_id": mongo_result['id']
        }

    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to transfer data to HDFS: {str(e)}"
        }
