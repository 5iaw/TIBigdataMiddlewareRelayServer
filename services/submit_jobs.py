# services/submit_jobs.py
import json
import requests
from flask import jsonify, request, Response
from requests.auth import HTTPBasicAuth
from services.webhdfs import webReadFile, webhdfs_request
from services.mongo_file import resolve_hdfs_paths
from datetime import datetime
from hdfs import InsecureClient

# Livy server URL and authentication
AUTH = HTTPBasicAuth('guest', 'guest-password')
LIVY_URL = "https://203.252.112.33:8443/gateway/sandbox/livy/v1/batches"

knox_url = "https://203.252.112.33:8443/gateway/sandbox/webhdfs/v1/"
client = InsecureClient(knox_url, user='ubuntu')

saved_file_ids = [] 
input_file_paths = []
inputs = []
import account.MongoAccount as monAcc
from pymongo import MongoClient

client = MongoClient(monAcc.host, monAcc.port)
db = client.user
dbTM = client.textMining

def test_mongo_connection():
    try:
        # List all databases to verify connection
        print("Databases:", client.list_database_names())

        # Access specific collections
        print("Collections in 'user':", db.list_collection_names())
        print("Collections in 'textMining':", dbTM.list_collection_names())

        return "Connection successful!"
    except Exception as e:
        return f"Connection failed: {e}"

def getPreprocessing(email, keyword, savedDate):
    docs = dbTM.preprocessing.find({"userEmail":email, "keyword":keyword, "savedDate":savedDate}).sort("_id", -1).limit(1)# saved date issue
    # print(email, keyword, savedDate)
    # print(docs[0]['titleList'])
    result = {
        "tokenList": docs[0]['tokenList'],
        "nTokens": docs[0]['nTokens']
    }
    return json.dumps(result)

def getPreprocessingAddTitle(email, keyword, savedDate, optionList):
    doc = dbTM.preprocessing.find({"userEmail":email, "keyword":keyword, "savedDate":savedDate, "addTitle" : "Yes"}).sort("_id", -1).limit(1)# saved date issue
    result = {
        "tokenList": doc[0]['tokenList'],
        "titleList": doc[0]['titleList'],
        "nTokens": doc[0]['nTokens']
    }
    return json.dumps(result)

def save_content_to_hdfs(json_content, path):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    hdfs_dest_path = f"{path}/input-files/{timestamp}"  # Path in HDFS, file ID as the filename
    
    try:
        # Write the content to the HDFS destination path
        # client.write(hdfs_dest_path, content, overwrite=True, verify=False)
        content_utf8 = json_content.encode('utf-8')
        webhdfs_request(hdfs_dest_path, 'CREATE', method='PUT', data=content_utf8, verify=False, auth=AUTH)
        print(f"File saved to HDFS at '{hdfs_dest_path}'")
        return hdfs_dest_path
    except Exception as e:
        print(f"Failed to save file to HDFS: {str(e)}")

def save_file_to_hdfs(file_id, content, path):
    """
    Save file to HDFS using the file content and file ID as the filename.
    """
    global input_file_paths
    hdfs_dest_path = f"{path}/{file_id}"  # Path in HDFS, file ID as the filename
    
    try:
        # Write the content to the HDFS destination path
        # client.write(hdfs_dest_path, content, overwrite=True, verify=False)
        content_utf8 = content.encode('utf-8')
        webhdfs_request(hdfs_dest_path, 'CREATE', method='PUT', data=content_utf8, verify=False, auth=AUTH)
        print(f"File saved to HDFS at '{hdfs_dest_path}'")
        input_file_paths.append(hdfs_dest_path)
    except Exception as e:
        print(f"Failed to save file to HDFS: {str(e)}")

def save_input_files():
    global saved_file_ids
    data = request.json  # Get the JSON data from the request body
    files = data.get('files', [])
    path = data.get('path', '/users/kubicuser/input-files')  # Default path if not provided
    
    if not files:
        return jsonify({"success": False, "message": "No files provided."}), 400
    
    saved_file_ids = [file['id'] for file in files]  # Save the file IDs for later use
    print("Saved file IDs:", saved_file_ids)

    # Iterate through each file, retrieve its content, and save it to HDFS
    for file in files:
        file_id = file.get('id')  # Get the file ID
        content = file.get('content')  # Get the file content
        
        if file_id and content:
            save_file_to_hdfs(file_id, content, path)  # Save the file content to HDFS
        else:
            print(f"Invalid file data for file ID {file_id}, skipping...")

    return jsonify({"success": True, "message": "Files saved to HDFS successfully."})

# def save_input_files():
#     global saved_file_ids
#     data = request.json
#     file_ids = data.get('files', [])
#     path = data.get('path', '/users/kubicuser/input-files')
    
#     if not file_ids:
#         return jsonify({"success": False, "message": "No file IDs provided."}), 400
    
#     saved_file_ids = file_ids  # Save the file IDs for later use
#     print("Saved file IDs:", saved_file_ids)


#     return jsonify({"success": True, "message": "File IDs saved successfully."})

def submit_job():
    global inputs

    data = request.json
    owner = data.get('userEmail')
    keyword = data.get('keyword')
    savedDate = data.get('savedDate')
    option1 = data.get('option1')
    option2 = data.get('option2')
    option3 = data.get('option3')
    analysis = data.get('analysisName')

    print("Received: ", owner, keyword, savedDate, option1, option2, option3, analysis)

    match analysis:
        case 'count':
            inputs = getPreprocessing(owner, keyword, savedDate)
            print("got inputs, going into submit_worcount_job")
            return submit_wordcount_job(owner, inputs, option1)
        case 'tfidf':
            inputs = getPreprocessing(owner, keyword, savedDate)
            return submit_tfidf_job(owner, inputs, option1)
        case 'network':
            inputs = getPreprocessing(owner, keyword, savedDate)
            return submit_sma_job(owner, inputs, option1, option2)
        case 'ngrams':
            inputs = getPreprocessing(owner, keyword, savedDate)
            return submit_ngrams_job(owner, inputs, option1, option3)
        case 'kmeans':
            inputs = getPreprocessingAddTitle(owner, keyword, savedDate)
            return submit_wordcount_job(owner, inputs, option1)
        case 'word2vec':
            inputs = getPreprocessing(owner, keyword, savedDate)
            return submit_ngrams_job(owner, inputs, option1)
        case 'hcluster':
            inputs = getPreprocessingAddTitle(owner, keyword, savedDate)
            return submit_wordcount_job(owner, inputs)
        case 'topicLDA':
            inputs = getPreprocessing(owner, keyword, savedDate)
            return submit_ngrams_job(owner, inputs, option1)
        case 'NER':
            return "Option 3 selected"
        case 'sentiment':
            return "Option 3 selected"
        case _:
            return "Invalid option"

def submit_wordcount_job(owner, inputs, option1):
    # global saved_file_ids
    # if not saved_file_ids:
    #     return jsonify({"error": "No file IDs have been saved. Please upload files first."}), 400

    # print(saved_file_ids)
    # data = request.json
    # if 'display_value' not in data:
    #     return jsonify({"error": "Missing parameter 'display_value'"}), 400

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    # owner = data.get('userEmail', 'kubicuser')

    path = f"/users/{owner}/inputs/{timestamp}"

    output_path =  f"/users/{owner}/analysis/{timestamp}wordcount"
    full_path = f"hdfs://Master1:9000{output_path}"
    input_path = save_content_to_hdfs(inputs, path)
    # input_files = ["hdfs://Master1:9000/example_txt/sample2.txt"]
    # input_files = input_file_paths
    print("Sending input files ")
    k_value = str(option1)
    payload = {
        "file": "hdfs://Master1:9000/algorithms/new_wc.py",
        "args": [k_value] + [full_path] + [input_path]
    }

    print("Sending Spark job: ", payload)

    headers = {'Content-Type': 'application/json'}
    response = requests.post(LIVY_URL, data=json.dumps(payload), headers=headers, auth=AUTH, verify=False)
    output = f"{output_path}/part-00003"

    saved_file_ids = [] 
    return handle_response(response, output)


# def submit_wordcount_job():
#     data = request.json
#     if 'display_value' not in data:
#         return jsonify({"error": "Missing parameter 'display_value'"}), 400

#     timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
#     owner = data.get('userEmail', 'kubicuser')

#     output_path =  f"/user/{owner}/analysis/{timestamp}wordcount"
#     full_path = f"hdfs://Master1:9000{output_path}"
#     input_files = ["hdfs://Master1:9000/example_txt/sample2.txt"]
#     k_value = str(data['display_value'])
#     payload = {
#         "file": "hdfs://Master1:9000/algorithms/new_wc.py",
#         "args": [k_value] + [full_path] + input_files 
#     }
#     headers = {'Content-Type': 'application/json'}
#     response = requests.post(LIVY_URL, data=json.dumps(payload), headers=headers, auth=AUTH, verify=False)
#     output = f"{output_path}/part-00003"
#     return handle_response(response, output)

def submit_kmeans_job(owner, inputs, option1):
    data = request.json
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('userEmail', 'kubicuser')

    output_path =  f"/user/{owner}/analysis/{timestamp}kmeans"
    full_path = f"hdfs://Master1:9000{output_path}"

    k_value = str(data.get('k_value', 3))
    input_file = input_file_paths
    # input_file = ["hdfs://Master1:9000/example_txt/alice.txt", "hdfs://Master1:9000/example_txt/deer.txt"]
    payload = {
        "file": "hdfs://Master1:9000/algorithms/new_kmeans.py",
        "args": [k_value] + [full_path] + input_file
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(LIVY_URL, data=json.dumps(payload), headers=headers, auth=AUTH, verify=False)
    output = f"{output_path}/part-00003"
    return handle_response(response, output)

def submit_w2v_job(owner, inputs, option1):
    data = request.json
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('userEmail', 'kubicuser')

    output_path =  f"/user/{owner}/analysis/{timestamp}w2v"
    full_path = f"hdfs://Master1:9000{output_path}"

    w2v_value = str(data.get('w2v_param', 3))
    input_file = input_file_paths
    # input_file = ["hdfs://Master1:9000/example_txt/alice.txt", "hdfs://Master1:9000/example_txt/deer.txt"]
    payload = {
        "file": "hdfs://Master1:9000/algorithms/new_w2v.py",
        "args": [w2v_value] + [full_path] + input_file
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(LIVY_URL, data=json.dumps(payload), headers=headers, auth=AUTH, verify=False)
    output = f"{output_path}/part-00003"
    return handle_response(response, output)

def submit_tfidf_job(owner, inputs, option1):
    data = request.get_json()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('userEmail', 'kubicuser')

    output_path =  f"/user/{owner}/analysis/{timestamp}tfidf"
    full_path = f"hdfs://Master1:9000{output_path}"
    
    k_value = str(data.get('tfidf_param', 3))
    input_file = input_file_paths
    # input_file = ["hdfs://Master1:9000/example_txt/alice.txt", "hdfs://Master1:9000/example_txt/deer.txt"]
    payload = {
        "file": "hdfs://Master1:9000/algorithms/new_tfidf.py",
        "args": [k_value] + [full_path] + input_file
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(LIVY_URL, data=json.dumps(payload), headers=headers, auth=AUTH, verify=False)
    output = f"{output_path}/part-00003"
    return handle_response(response, output)

def submit_lda_job(owner, inputs, option1):
    data = request.get_json()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('userEmail', 'kubicuser')

    output_path =  f"/user/{owner}/analysis/{timestamp}lda"
    full_path = f"hdfs://Master1:9000{output_path}"
    
    k_value = str(data.get('lda_param', 5))
    input_file = input_file_paths
    # input_file = ["hdfs://Master1:9000/example_txt/alice.txt", "hdfs://Master1:9000/example_txt/deer.txt"]
    payload = {
        "file": "hdfs://Master1:9000/algorithms/new_lda.py",
        "args": [k_value] + [full_path] + input_file
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(LIVY_URL, data=json.dumps(payload), headers=headers, auth=AUTH, verify=False)
    output = f"{output_path}/part-00003"
    return handle_response(response, output)

def submit_sma_job(owner, inputs, option1, option2):
    data = request.get_json()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('userEmail', 'kubicuser')

    output_path =  f"/user/{owner}/analysis/{timestamp}sma"
    full_path = f"hdfs://Master1:9000{output_path}"
    
    optionList = str(data.get('optionList', 2))
    linkStrength = str(data.get('linkStrength', '0.5'))
    input_file = input_file_paths
    # input_file = ["hdfs://Master1:9000/example_txt/alice.txt", "hdfs://Master1:9000/example_txt/deer.txt"]
    payload = {
        "file": "hdfs://Master1:9000/algorithms/new_sma.py",
        "args": [optionList, linkStrength] + [full_path] + input_file
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(LIVY_URL, data=json.dumps(payload), headers=headers, auth=AUTH, verify=False)
    output = f"{output_path}/part-00003"
    return handle_response(response, output)

def submit_ngrams_job(owner, inputs, option1, option3):
    data = request.get_json()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('userEmail', 'kubicuser')

    output_path =  f"/user/{owner}/analysis/{timestamp}ngrams"
    full_path = f"hdfs://Master1:9000{output_path}"
    
    optionList = str(data.get('optionList', 5))
    n = str(data.get('n', 2))
    linkStrength = str(data.get('linkStrength', '0.5'))
    input_file = input_file_paths
    # input_file = ["hdfs://Master1:9000/example_txt/alice.txt", "hdfs://Master1:9000/example_txt/deer.txt"]
    payload = {
        "file": "hdfs://Master1:9000/algorithms/ngrams.py",
        "args": [optionList, n, linkStrength] + [full_path] + input_file
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(LIVY_URL, data=json.dumps(payload), headers=headers, auth=AUTH, verify=False)
    output = f"{output_path}/part-00003"
    return handle_response(response, output)

def submit_hclustering_job(owner, inputs):
    data = request.get_json()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('userEmail', 'kubicuser')

    output_path =  f"/user/{owner}/analysis/{timestamp}hclustering"
    full_path = f"hdfs://Master1:9000{output_path}"
    
    k_value = str(5)
    input_file = input_file_paths
    # input_file = ["hdfs://Master1:9000/example_txt/alice.txt", "hdfs://Master1:9000/example_txt/deer.txt"]
    payload = {
        "file": "hdfs://Master1:9000/algorithms/new_hc.py",
        "args": [k_value] + [full_path] + input_file
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(LIVY_URL, data=json.dumps(payload), headers=headers, auth=AUTH, verify=False)
    output = f"{output_path}/part-00003"
    return handle_response(response, output)

def submit_ner_job():
    data = request.get_json()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('userEmail', 'kubicuser')

    output_path =  f"/user/{owner}/analysis/{timestamp}ner"
    full_path = f"hdfs://Master1:9000{output_path}"
    
    k_value = "NOUN,PER,ORG,LOC,GPE,PRODUCT"
    optionList = str(data.get('ner_param', 10))

    input_file = input_file_paths

    # input_file = ["hdfs://Master1:9000/example_txt/copy1.txt"]
    payload = {
        "file": "hdfs://Master1:9000/algorithms/ner.py",
        "args": [optionList, k_value] + [full_path] + input_file
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(LIVY_URL, data=json.dumps(payload), headers=headers, auth=AUTH, verify=False)
    output = f"{output_path}/part-00003"
    return handle_response(response, output)

def get_status(batch_id):
    status_url = f"{LIVY_URL}/{batch_id}"
    response = requests.get(status_url, auth=AUTH, verify=False)
    return handle_response(response)

def get_log(batch_id):
    log_url = f"{LIVY_URL}/{batch_id}/log"
    response = requests.get(log_url, auth=AUTH, verify=False)
    return handle_response(response)

def handle_response(response, output_path=None):
    if response.status_code == 201:
        result = response.json()
        if output_path:
            result['output_path'] = output_path
        return jsonify(result), 201
    elif response.status_code == 200:
        result = response.json()
        if output_path:
            result['output_path'] = output_path
        return jsonify(result), 200
    else:
        try:
            error_details = response.json()
            if not isinstance(error_details, dict):
                error_details = {"message": str(error_details)}
        except ValueError:
            error_details = {"message": response.text}

        if output_path:
            error_details['output_path'] = output_path

        return jsonify({"error": "Failed to process request", "details": error_details}), response.status_code

def get_analysis_result():
    # Call the appropriate function to get the result
    # directory_path = request.get_json()
    # # result = webReadFile(directory_path)  # Replace with the actual function
    # hdfs_read_file_route(directory_path)
    # return jsonify(result)
    path = request.args.get('output_path')
    
    if not path:
        return jsonify({"error": "Output path is required"}), 400

    result = webReadFile(path)
    
    if 'file_content' in result:
        # Return the file content directly as a binary response
        return Response(result['file_content'], mimetype='text/plain')
    else:
        return jsonify(result), 500


def hdfs_read_file_route(path):
    result = webReadFile(path)
    if 'file_content' in result:
        # Return the file content directly as a binary response
        return Response(result['file_content'], mimetype='text/plain')
    else:
        return jsonify(result), 500

def test_connection():
    return jsonify({"message": "Middleware is up and running"}), 200
