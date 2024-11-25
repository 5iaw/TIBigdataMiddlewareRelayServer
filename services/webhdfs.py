# webhdfs.py
from flask import jsonify
import requests
import os
from requests.auth import HTTPBasicAuth

# Authentication for Knox
AUTH = HTTPBasicAuth('guest', 'guest-password')

WEBHDFS_URL = "https://203.252.112.33:8443/gateway/sandbox/webhdfs/v1"

def webhdfs_request(path, op, method='GET', data=None, params=None, headers=None, verify=False, auth=AUTH):
    user_name = 'ubuntu'  # Ensuring the user is 'ubuntu'
    url = f"{WEBHDFS_URL}{path}?op={op}&user.name={user_name}"

    if params:
        url += '&' + '&'.join(f"{key}={value}" for key, value in params.items())

    # Use the provided auth or fallback to the default AUTH
    current_auth = auth if auth else AUTH

    if method == 'GET':
        response = requests.get(url, headers=headers, verify=verify, auth=current_auth)
    elif method == 'PUT':
        response = requests.put(url, headers=headers, data=data, verify=verify, auth=current_auth)
    elif method == 'DELETE':
        response = requests.delete(url, verify=verify, auth=current_auth)
    else:
        raise ValueError("Unsupported HTTP method")

    if response.status_code in range(200, 300):
        return response
    else:
        raise Exception(f"WebHDFS request failed with status {response.status_code}: {response.text}")

def webListDirectory(directory_path: str) -> dict:
    try:
        response = webhdfs_request(directory_path, 'LISTSTATUS', method='GET')
        directory_contents = response.json()['FileStatuses']['FileStatus']
        return {'directory_contents': directory_contents}
    except Exception as e:
        return {'error': str(e)}

def webReadFile(file_path: str) -> dict:
    try:
        print(f" webReadFile function file_path = {file_path}" )
        response = webhdfs_request(file_path, 'OPEN', method='GET', headers={'Accept': 'application/octet-stream'})
        response_content = response.content  # Use .content to handle binary data
        return {'file_content': response_content}
    except Exception as e:
        return {'error': str(e)}

def webUploadFileToHDFS(local_file_path: str, hdfs_dest_path: str) -> dict:
    try:
        project_root = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(project_root, local_file_path)
        if not os.path.exists(path):
            return {'success': False, 'message': f"Local file {path} does not exist"}

        with open(path, 'rb') as file_data:
            webhdfs_request(hdfs_dest_path, 'CREATE', method='PUT', data=file_data, verify=False, auth=AUTH)
        
        # Get the file size from HDFS
        status_response = webGetFileStatus(hdfs_dest_path)
        if status_response['success']:
            file_size = status_response['size']
        else:
            return {
                'success': False,
                'message': f"File uploaded but failed to retrieve size: {status_response['message']}"
            }

        return {
            'success': True,
            'message': f"Successfully uploaded {path} to HDFS at {hdfs_dest_path}",
            'local_file_path': path,
            'hdfs_dest_path': hdfs_dest_path,
            'file_size': file_size  # Return the size of the uploaded file
        }
    except Exception as e:
        return {
            'success': False,
            'message': f"Failed to upload {path} to HDFS: {str(e)}"
        }

def webDeleteFileFromHDFS(hdfs_file_path: str) -> dict:
    try:
        webhdfs_request(hdfs_file_path, 'DELETE', method='DELETE', params={'recursive': 'false'}, verify=False, auth=AUTH)
        return {'success': True, 'message': f"Successfully deleted {hdfs_file_path} from HDFS"}
    except Exception as e:
        return {'success': False, 'message': f"Failed to delete {hdfs_file_path} from HDFS: {str(e)}"}

def webDeleteFolderFromHDFS(hdfs_folder_path: str) -> dict:
    try:
        webhdfs_request(hdfs_folder_path, 'DELETE', method='DELETE', params={'recursive': 'true'}, verify=False, auth=AUTH)
        return {'success': True, 'message': f"Successfully deleted {hdfs_folder_path} from HDFS"}
    except Exception as e:
        return {'success': False, 'message': f"Failed to delete {hdfs_folder_path} from HDFS: {str(e)}"}

def webCreateFolderInHDFS(hdfs_folder_path: str) -> dict:
    try:
        response = webhdfs_request(hdfs_folder_path, 'MKDIRS', method='PUT', verify=False, auth=AUTH)
        print(response)
        return {
            'success': True,
            'message': f"Successfully created folder {hdfs_folder_path} in HDFS",
            'hdfs_folder_path': hdfs_folder_path
        }
    except Exception as e:
        return {
            'success': False,
            'message': f"Failed to create folder {hdfs_folder_path} in HDFS: {str(e)}"
        }

def webMoveFolderInHDFS(source_path: str, destination_path: str) -> dict:
    try:
        response = webhdfs_request(source_path, 'RENAME', method='PUT', params={'destination': destination_path}, verify=False, auth=AUTH)
        return {
            'success': True,
            'message': f"Successfully moved folder from {source_path} to {destination_path}",
            'source_path': source_path,
            'destination_path': destination_path
        }
    except Exception as e:
        return {
            'success': False,
            'message': f"Failed to move folder from {source_path} to {destination_path}: {str(e)}"
        }

def webMoveFileInHDFS(source_path: str, destination_path: str) -> dict:
    try:
        response = webhdfs_request(source_path, 'RENAME', method='PUT', params={'destination': destination_path}, verify=False, auth=AUTH)
        return {
            'success': True,
            'message': f"Successfully moved file from {source_path} to {destination_path}",
            'source_path': source_path,
            'destination_path': destination_path
        }
    except Exception as e:
        return {
            'success': False,
            'message': f"Failed to move file from {source_path} to {destination_path}: {str(e)}"
        }

def webGetFileStatus(file_path: str) -> dict:
    try:
        response = webhdfs_request(file_path, 'GETFILESTATUS', method='GET', auth=AUTH)
        file_status = response.json().get('FileStatus', {})
        return {
            'success': True,
            'size': file_status.get('length'),
            'file_status': file_status
        }
    except Exception as e:
        return {'success': False, 'message': f"Failed to get file status for {file_path}: {str(e)}"}

def createDirectoryIfNotExists(directory_path: str) -> dict:
    try:
        # Check if the directory already exists in HDFS
        response = webhdfs_request(directory_path, 'GET', method='GET')
        if response.status_code == 200:
            return {"success": True, "message": "Directory already exists"}
        
        # If not, create the directory
        response = webhdfs_request(directory_path, 'MKDIRS', method='PUT')
        if response.status_code == 200:
            return {"success": True, "message": "Directory created successfully"}
        else:
            return {"success": False, "message": "Failed to create directory"}
    except Exception as e:
        return {"success": False, "message": str(e)}
