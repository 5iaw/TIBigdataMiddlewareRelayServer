# 기존 flask에서 사용한 모듈 전체 적용
from flask import (
    Flask,
    jsonify,
    request,
    Response,
    render_template,
    copy_current_request_context,
    current_app,
    abort,
)

import requests
import json
import account.BE_flask as BE

from urllib.parse import quote_plus

from bson.objectid import ObjectId
import os

from pymongo import MongoClient
from services.es_to_hdfs import *
from services.mongo_file import (
    create_folder,
    delete_file_folder_by_id_with_hdfs,
    insert_file_folder,
    move_file_or_folder_by_id,
    rename_file_or_folder_by_id,
    resolve_hdfs_paths,
    update_file_folder,
    delete_file_folder,
    list_folder_contents,
)
from services.webhdfs import (
    webCreateFolderInHDFS,
    webReadFile,
    webUploadFileToHDFS,
)

from services.input_submit_jobs import *

from services.submit_jobs import *
from services.es import *

from flask.blueprints import Blueprint

from requests.auth import HTTPBasicAuth

MONGO_URI = f"mongodb://localhost:27017/"

# If your local MongoDB requires authentication, include username and password:
# MONGO_URI = f"mongodb://username:{escaped_password}@localhost:27017/"

client = MongoClient(MONGO_URI)
db = client["unik"]
file_collection = db["files_folders"]
# flask 객체
app = Flask(__name__)  # 정적 파일과 템플릿을 찾는데 쓰인다고 한다. 무슨소리일까..


@app.after_request
def after_request(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
    return response


@app.route("/preprocessing", methods=["GET", "POST"])
def preprocessing():
    print("/preprocessing" + "request" + "recieved")
    if request.method == "POST":
        data = request.json
        print(data)
    r = requests.post(
        "https://" + BE.ip + ":" + BE.port + "/preprocessing", verify=False, json=data
    ).text
    return r


@app.route("/textmining", methods=["GET", "POST"])
def textmining():
    print("/textming" + "request" + "recieved")
    if request.method == "POST":
        data = request.json
        print(data)
    r = requests.post(
        "https://" + BE.ip + ":" + BE.port + "/textmining", verify=False, json=data
    ).text
    return r


@app.route("/countTable", methods=["GET", "POST"])
def countTable():
    # app = Flask(__name__)
    # app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
    print("*************/counTable request recieved")
    if request.method == "POST":
        data = request.json
        print(data)
    else:
        return "request method is not POST"
    r = requests.post(
        "https://" + BE.ip + ":" + BE.port + "/countTable", verify=False, json=data
    ).text
    return r


# @app.route('/test', methods=['GET', 'POST'])
# def test():
#     if request.method == 'GET':
#         r = requests.post("https://"+BE.ip+":"+BE.port+"/preprocessing",verify=False, json = {
#             "userEmail": "21800520@handong.edu",
#             "keyword": "북한",
#             "savedDate": "2021-09-07T07:01:07.137Z",
#             "synonym": False,
#             "stopword": False,
#             "compound": False,
#             "wordclass": "010"
#         })

#         print(r.text)

#         r = requests.post("https://"+BE.ip+":"+BE.port+"/textmining",verify=False, json ={
#             "userEmail": "21800520@handong.edu",
#             "keyword": "북한",
#             "savedDate": "2021-09-07T07:01:07.137Z",
#             "option1": "100",
#             "option2": None,
#             "option3": None,
#             "analysisName": "count"
#             }).text

#         print(r)
#         return r
#     elif request.method == 'POST':
#         data = request.json

#         return "사용자"+data["userEmail"]+"이(가) 확인되었습니다."


# Upload File
@app.route("/file/upload", methods=["POST"])
def upload_file():
    owner = request.form.get("owner")
    path = request.form.get("path")
    file = request.files.get("file")

    if not file:
        return jsonify({"success": False, "message": "No file provided"}), 400

    filename = file.filename
    local_file_path = f"/tmp/{filename}"
    file.save(local_file_path)

    # Get file size and store metadata in MongoDB
    file_size = os.path.getsize(local_file_path)
    result = insert_file_folder(
        name=filename,
        path=f"{path}/{filename}",
        hdfs_path="",
        owner=owner,
        type="file",
        size=file_size,
        parent_path=path,
    )
    if not result["success"]:
        os.remove(local_file_path)
        return (
            jsonify(
                {"success": False, "message": "Failed to insert metadata into storage"}
            ),
            500,
        )

    # Upload file to HDFS and update MongoDB with HDFS path
    mongo_id = result.get("id")
    storage_path = f"/users/{owner}/personal_files/{mongo_id}"
    hdfs_result = webUploadFileToHDFS(local_file_path, storage_path)
    os.remove(local_file_path)

    if not hdfs_result["success"]:
        delete_file_folder(f"{path}/{filename}")
        return (
            jsonify({"success": False, "message": "Failed to save file to storage"}),
            500,
        )

    update_file_folder(f"{path}/{filename}", {"hdfs_file_path": storage_path})
    return (
        jsonify(
            {
                "success": True,
                "message": "File uploaded successfully",
                "file_id": mongo_id,
            }
        ),
        201,
    )


# Delete File
# @file_management_bp.route('/delete', methods=['DELETE'])
# def delete_file():
#     path = request.args.get('path')
#     file_metadata = get_file_folder(path)
#     if not file_metadata['success']:
#         return jsonify({"success": False, "message": "File not found in storage."}), 404
#
#     storage_path = file_metadata['metadata'].get('hdfs_file_path')
#     delete_result = webDeleteFileFromHDFS(storage_path)
#     if not delete_result['success']:
#         return jsonify({"success": False, "message": "Failed to delete file from storage"}), 500
#
#     result = delete_file_folder(path)
#     return jsonify(result), 200 if result['success'] else 500
#
# Delete File or Folder by ID


@app.route("/file/delete", methods=["DELETE"])
def delete_file_or_folder():
    item_id = request.args.get("id")
    if not item_id:
        return jsonify({"success": False, "message": "Item ID is required"}), 400

    try:
        item_id = ObjectId(item_id)
    except Exception as e:
        return jsonify({"success": False, "message": "Invalid ID format"}), 400

    result = delete_file_folder_by_id_with_hdfs(item_id)
    status_code = 200 if result["success"] else 500
    return jsonify(result), status_code


# List Files in Folder
@app.route("/file/list", methods=["GET"])
def list_files():
    owner = request.args.get("owner")
    folder_path = request.args.get("folder_path", "/")
    result = list_folder_contents(owner, folder_path)

    if result["success"]:
        files = [
            {
                "id": f["id"],
                "name": f["name"],
                "type": f["type"],
                "size": f.get("size"),
                "created_at": f.get("created_at"),
                "updated_at": f.get("updated_at"),
                "hdfs_file_path": f.get("hdfs_file_path"),
                "path": f.get("path"),
            }
            for f in result.get("contents", [])
        ]
        return jsonify({"success": True, "files": files}), 200
    else:
        return (
            jsonify({"success": False, "message": "Could not retrieve file list"}),
            404,
        )


# Download File by MongoDB ID
@app.route("/file/download", methods=["GET"])
def download_file():
    file_id = request.args.get("id")
    if not file_id:
        return jsonify({"success": False, "message": "File ID is required"}), 400

    # Use resolve_hdfs_paths to get the HDFS path
    hdfs_paths = resolve_hdfs_paths([file_id])
    if not hdfs_paths:
        return (
            jsonify(
                {
                    "success": False,
                    "message": "No HDFS path found for the given file ID.",
                }
            ),
            404,
        )

    hdfs_path = hdfs_paths[0]  # Assuming only one file is requested

    try:
        # Read the file content from HDFS
        response = webReadFile(hdfs_path)
        if "error" in response:
            return jsonify({"success": False, "message": response["error"]}), 500

        file_content = response["file_content"]
        file_metadata = file_collection.find_one({"_id": ObjectId(file_id)})
        filename = file_metadata.get("name", "downloaded_file")

        # Determine the content type based on file extension
        content_type = "application/octet-stream"
        if filename.endswith(".png"):
            content_type = "image/png"
        elif filename.endswith(".jpeg") or filename.endswith(".jpg"):
            content_type = "image/jpeg"
        elif filename.endswith(".txt"):
            content_type = "text/plain"

        # Return file as attachment with appropriate headers
        return (
            file_content,
            200,
            {
                "Content-Type": content_type,
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
        )
    except Exception as e:
        # Log the exception for debugging
        print("Exception while reading file from HDFS:", e)
        return (
            jsonify({"success": False, "message": "Failed to retrieve file content"}),
            500,
        )


# Transfer ES Data to Storage
@app.route("/file/transfer", methods=["POST"])
def transfer_to_storage():
    es_id = request.form.get("es_id")
    owner = request.form.get("owner")

    if not es_id or not owner:
        return (
            jsonify(
                {
                    "success": False,
                    "message": "Elasticsearch ID and owner are required.",
                }
            ),
            400,
        )

    storage_dest_path = f"/users/{owner}/personal_files/es_data_{es_id}.json"
    result = transfer_es_data_to_hdfs_with_mongo(es_id, storage_dest_path, owner)
    return jsonify(result), 201 if result["success"] else 500


@app.route("/file/user/folder", methods=["GET"])
def view_user_folder():
    owner = request.args.get("owner")
    folder_path = request.args.get("folder_path", f"/users/{owner}")

    # Log initial request parameters
    print(
        f"[DEBUG] view_user_folder called with owner: {owner}, folder_path: {folder_path}"
    )

    if not owner:
        print("[ERROR] Owner is missing in the request")
        return jsonify({"success": False, "message": "Owner is required"}), 400

    # Define the directories that need to exist
    required_directories = [
        f"/users/{owner}",
        f"/users/{owner}/personal_files",
        f"/users/{owner}/analysis",
    ]

    # Log required directories
    print(f"[DEBUG] Required directories to check: {required_directories}")

    # Check and create each required directory if it does not exist
    for directory in required_directories:
        print(f"[DEBUG] Checking or creating directory: {directory}")
        directory_creation_result = webCreateFolderInHDFS(directory)

        # Log the result of the directory creation attempt
        print(
            f"[DEBUG] Directory creation result for {directory}: {directory_creation_result}"
        )

        if (
            not directory_creation_result["success"]
            and "already exists" not in directory_creation_result["message"]
        ):
            print(f"[ERROR] Failed to create directory: {directory}")
            return (
                jsonify(
                    {
                        "success": False,
                        "message": f"Failed to create directory: {directory}",
                    }
                ),
                500,
            )

    # Once directories are ensured, retrieve the folder contents
    print(f"[DEBUG] Attempting to list contents of folder: {folder_path}")
    folder_contents = list_folder_contents(owner, folder_path)

    # Log the result of folder contents retrieval
    print(f"[DEBUG] Folder contents retrieval result: {folder_contents}")

    # Modify contents to include analysis-related fields
    if folder_contents.get("success"):
        contents = [
            {
                "id": item["id"],
                "name": item["name"],
                "type": item["type"],
                "path": item["path"],
                "size": item.get("size", 0),
                "created_at": item.get("created_at"),
                "updated_at": item.get("updated_at"),
                "hdfs_file_path": item.get("hdfs_file_path"),
                # Explicitly fetch and log `is_analysis_result` and `analysis_result_type`
                "is_analysis_result": item.get("is_analysis_result", False),
                "analysis_result_type": item.get("analysis_result_type", None),
            }
            for item in folder_contents["contents"]
        ]

        # Debug logs
        print(f"[DEBUG] Folder contents with analysis fields: {contents}")
        return jsonify({"success": True, "contents": contents}), 200
    else:
        print("[ERROR] Failed to retrieve folder contents.")
        return (
            jsonify(
                {"success": False, "message": "Failed to retrieve folder contents"}
            ),
            404,
        )


# Create a New Folder
@app.route("/file/folder/create", methods=["POST"])
def create_new_folder():
    owner = request.json.get("owner")
    path = request.json.get("path")
    folder_name = request.json.get("folder_name")

    if not owner or not path or not folder_name:
        return (
            jsonify(
                {
                    "success": False,
                    "message": "Owner, path, and folder_name are required",
                }
            ),
            400,
        )

    result = create_folder(owner=owner, parent_path=path, folder_name=folder_name)
    if result["success"]:
        return jsonify({"success": True, "message": "Folder created successfully"}), 201
    else:
        return jsonify({"success": False, "message": "Failed to create folder"}), 500


# Move a File or Folder
@app.route("/file/move", methods=["PUT"])
def move_file_or_folder_route():
    owner = request.json.get("owner")
    item_id = request.json.get("id")
    new_parent_path = request.json.get("new_parent_path")

    if not owner or not item_id or not new_parent_path:
        return (
            jsonify(
                {
                    "success": False,
                    "message": "Owner, item ID, and new parent path are required",
                }
            ),
            400,
        )

    # Convert item_id to ObjectId for MongoDB query
    try:
        item_id = ObjectId(item_id)
    except Exception as e:
        return jsonify({"success": False, "message": "Invalid item ID format"}), 400

    # Call the move function and return the result
    result = move_file_or_folder_by_id(item_id, new_parent_path)
    status_code = 200 if result["success"] else 500
    return jsonify(result), status_code


# Rename a File or Folder
@app.route("/file/rename", methods=["POST"])
def rename_file_or_folder_route():
    data = request.json
    item_id = data.get("id")
    new_name = data.get("new_name")

    if not item_id or not new_name:
        return (
            jsonify({"success": False, "message": "ID and new name are required"}),
            400,
        )

    result = rename_file_or_folder_by_id(item_id, new_name)
    return jsonify(result)


# input_livy_routes.py


# Livy server URL
AUTH = HTTPBasicAuth("guest", "guest-password")
LIVY_URL = "https://localhost:8443/gateway/sandbox/livy/v1/batches"

input_livy_bp = Blueprint("input_livy_bp", __name__)


@app.route("/livy/submit_wordcount", methods=["POST"])
def route_submit_wordcount_job1():
    return submit_wordcount_job1()


@app.route("/livy/submit_kmeans", methods=["POST"])
def route_submit_kmeans_job1():
    return submit_kmeans_job1()


@app.route("/livy/submit_w2v", methods=["POST"])
def route_submit_w2v_job1():
    return submit_w2v_job1()


@app.route("/livy/submit_tfidf", methods=["POST"])
def route_submit_tfidf_job1():
    return submit_tfidf_job1()


@app.route("/livy/submit_lda", methods=["POST"])
def route_submit_lda_job1():
    return submit_lda_job1()


@app.route("/livy/submit_sma", methods=["POST"])
def route_submit_sma_job1():
    return submit_sma_job1()


@app.route("/livy/submit_ngrams", methods=["POST"])
def route_submit_ngrams_job1():
    return submit_ngrams_job1()


@app.route("/livy/submit_hclustering", methods=["POST"])
def route_submit_hclustering_job1():
    return submit_hclustering_job1()


@app.route("/livy/submit_ner", methods=["POST"])
def route_submit_ner_job1():
    return submit_ner_job1()


@app.route("/livy/status/<int:batch_id>", methods=["GET"])
def route_get_status1(batch_id):
    owner = request.args.get("owner")
    return get_status1(batch_id, owner=owner)


@app.route("/livy/log/<int:batch_id>", methods=["GET"])
def route_get_log1(batch_id):
    owner = request.args.get("owner")
    return get_log1(batch_id, owner=owner)


@app.route("/livy/analysis", methods=["GET"])
def route_get_analysis_result1():
    return get_analysis_result1()


@app.route("/livy/test-connection", methods=["GET"])
def route_test_connection1():
    return test_connection1()

# es file analysis
@app.route('/es/connTest', methods=['GET'])
def esTest_routes():
    return esTest()

@app.route('/es/esQuery', methods=['POST'])
def es_Query_routes():
    return es_query()

@app.route('/es/ping', methods=['GET'])
def ping_routes():
    return ping()

@app.route('/es/count',  methods=['POST'])
def count_routes():
    return count()

@app.route('/es/search',  methods=['POST'])
def search_routes():
    return search()


@app.route('/spark/submit_job', methods=['POST'])
def route_submit_job():
    return submit_job()

@app.route('/spark/input-files', methods=['POST'])
def route_input_files():
    return save_input_files()

@app.route('/spark/submit_wordcount', methods=['POST'])
def route_submit_wordcount_job():
    return submit_wordcount_job()

@app.route('/spark/submit_kmeans', methods=['POST'])
def route_submit_kmeans_job():
    return submit_kmeans_job()

@app.route('/spark/submit_w2v', methods=['POST'])
def route_submit_w2v_job():
    return submit_w2v_job()

@app.route('/spark/submit_tfidf', methods=['POST'])
def route_submit_tfidf_job():
    return submit_tfidf_job()

@app.route('/spark/submit_lda', methods=['POST'])
def route_submit_lda_job():
    return submit_lda_job()

@app.route('/spark/submit_sma', methods=['POST'])
def route_submit_sma_job():
    return submit_sma_job()

@app.route('/spark/submit_ngrams', methods=['POST'])
def route_submit_ngrams_job():
    return submit_ngrams_job()

@app.route('/spark/submit_hclustering', methods=['POST'])
def route_submit_hclustering_job():
    return submit_hclustering_job()

@app.route('/spark/submit_ner', methods=['POST'])
def route_submit_ner_job():
    return submit_ner_job()

@app.route('/spark/status/<int:batch_id>', methods=['GET'])
def route_get_status(batch_id):
    return get_status(batch_id)

@app.route('/spark/log/<int:batch_id>', methods=['GET'])
def route_get_log(batch_id):
    return get_log(batch_id)

@app.route('/spark/analysis', methods=['GET'])
def route_get_analysis_result():
    return get_analysis_result()

@app.route('/spark/test-connection', methods=['GET'])
def route_test_connection():
    return test_connection()

@app.route('/spark/read_file', methods=['GET'])
def webhdfs_read_file_route():
    output_path = request.args.get('output_path', '')
    print(f"Received output_path: {output_path}")  # Debugging print
    try:
        result = webReadFile(output_path)
        if 'file_content' in result:
            return Response(result['file_content'], mimetype='text/plain')
        else:
            return jsonify(result), 500
    except Exception as e:
        print("Error in webhdfs_read_file_route:", str(e))
        return jsonify({'error': 'Failed to read file from HDFS', 'details': str(e)}), 500

@app.route('/spark/test-mongo', methods=['GET'])
def route_test_mongo():
    return test_mongo_connection()

@app.route('/spark/get-preprocessing', methods=['POST'])
def route_get_preprocessing():
    email = "22100409@handong.ac.kr"
    keyword = "no"
    savedDate = "2024-08-16T01:52:21.789Z"
    return getPreprocessing(email, keyword, savedDate)
    # return jsonify({'docs': docs, 'nTokens': nTokens})

@app.route('/spark/get-preprocessing-title', methods=['POST'])
def route_get_preprocessing_add_title():
    email = "22100409@handong.ac.kr"
    keyword = "no"
    savedDate = "2024-08-16T01:52:21.789Z"
    return getPreprocessingAddTitle(email, keyword, savedDate)
    # return jsonify({'docs': docs, 'nTokens': nTokens})

import account.FE_flask as FERS
import account.kubic_sslFile as kubic_ssl

if __name__ == "__main__":

    context = (kubic_ssl.crt, kubic_ssl.key)
    app.run(host=FERS.hostIp, port=FERS.port, ssl_context=context, debug=True)
