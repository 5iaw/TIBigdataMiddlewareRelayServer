from flask import jsonify
from hdfs import InsecureClient
import os

from app.services.file_system import save_uploaded_file
knox_url = "https://localhost:8443/gateway/sandbox/webhdfs/v1/"
client = InsecureClient(knox_url, user='ubuntu')

import requests

from requests.auth import HTTPBasicAuth

auth = HTTPBasicAuth('guest', 'guest-password')

def list_directory() -> dict:
    try:
        directory_contents = client.list('/flask')
        return jsonify({'directory_contents': directory_contents}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def read_file() -> dict:
    try:
        with client.read('/flask/example.json', encoding='utf-8') as reader:
            content = reader.read()
        return jsonify({'file_content': content}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def upload_file_to_hdfs(local_file_path: str, hdfs_dest_path: str) -> dict:
    try:
        # Check if the local file exists
        if not os.path.exists(local_file_path):
            return {
                "success": False,
                "message": f"Local file {local_file_path} does not exist"
            }
        
        # Open the local file and upload to HDFS
        with open(local_file_path, 'rb') as file_data:
            client.write(hdfs_dest_path, file_data, overwrite=True)
        
        return {
            "success": True,
            "message": f"Successfully uploaded {local_file_path} to HDFS at {hdfs_dest_path}",
            "local_file_path": local_file_path,
            "hdfs_dest_path": hdfs_dest_path
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to upload {local_file_path} to HDFS: {str(e)}",
            "local_file_path": local_file_path,
            "hdfs_dest_path": hdfs_dest_path
        }


def upload_multiple_files(files,hdfs_dest_path: str, upload_folder: str) -> list:

    uploaded_files = []
    for file in files:
        if file and file.filename != '':

            local_file_path = save_uploaded_file(file, upload_folder)

            upload_result = upload_file(local_file_path, hdfs_dest_path)

            delete_local_file(local_file_path)

            uploaded_files.append(upload_result)
    return uploaded_files



def delete_file_from_hdfs(hdfs_file_path: str) -> dict:
    
    try:
        # Check if the file exists
        if not client.status(hdfs_file_path, strict=False):
            return jsonify({"success": False, "message": "File does not exist in HDFS", "hdfs_file_path": hdfs_file_path}), 404

        # Delete the file from HDFS
        client.delete(hdfs_file_path, recursive=False)
        return jsonify({
            "success": True,
            "message": f"Successfully deleted {hdfs_file_path} from HDFS",
            "hdfs_file_path": hdfs_file_path
        }), 200
    except Exception as e:
        return jsonify({
            "success": False,
            "message": f"Failed to delete {hdfs_file_path} from HDFS: {str(e)}",
            "hdfs_file_path": hdfs_file_path
        }), 500





