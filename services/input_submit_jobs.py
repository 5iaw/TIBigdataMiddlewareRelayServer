# Updated /services/input_submit_jobs.py

import json
import os
from urllib.parse import quote_plus
from pymongo import MongoClient
import requests
from flask import jsonify, request, Response
from requests.auth import HTTPBasicAuth
from services.mongo_file import resolve_hdfs_paths
from services.webhdfs import webReadFile
from datetime import datetime
import time
from bson.objectid import ObjectId

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017/"
client = MongoClient(MONGO_URI)
db = client['unik']
file_collection = db['files_folders']

# Livy server URL and authentication
AUTH = HTTPBasicAuth('guest', 'guest-password')
LIVY_URL = "https://localhost:8443/gateway/sandbox/livy/v1/batches"

def submit_spark_job1(job_name, file_path, args, output_path=None, parent_path=None, owner=None, analysis_type=None):
    payload = {
        "file": file_path,
        "args": args,
        "proxyUser": "guest"
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(LIVY_URL, data=json.dumps(payload), headers=headers, auth=AUTH, verify=False)
    return handle_response1(response, output_path, parent_path=parent_path, owner=owner, analysis_type=analysis_type)

def handle_response1(response, output_path=None, parent_path=None, owner=None, retry_interval=10, max_retries=12, analysis_type=None):
    if response.status_code in [200, 201]:
        result = response.json()
        if output_path:
            result['output_path'] = output_path

        cleaned_output_path = output_path.replace("hdfs://Master1:9000", "")
        analysis_file_path = f"{cleaned_output_path}/part-00003"
        
        for attempt in range(max_retries):
            print(f"Attempt {attempt + 1}: Reading {analysis_file_path}")
            read_result = webReadFile(analysis_file_path)
            if 'file_content' in read_result:
                timestamp = datetime.utcnow()
                job_name = output_path.split('/')[-1]
                
                file_metadata = {
                    "name": job_name,
                    "parent_path": parent_path,
                    "path": f"{parent_path}/{job_name}",
                    "hdfs_file_path": analysis_file_path,
                    "owner": owner,
                    "type": "file",
                    "is_analysis_result": analysis_type is not None,
                    "analysis_result_type": analysis_type,
                    "created_at": timestamp,
                    "size": len(read_result['file_content']),
                }

                try:
                    insert_result = file_collection.insert_one(file_metadata)
                    inserted_id = insert_result.inserted_id
                    return jsonify({
                        "success": True,
                        "output_path": analysis_file_path,
                        "document_id": str(inserted_id),
                        "message": "File metadata added to MongoDB."
                    }), response.status_code
                except Exception as e:
                    return jsonify({"error": "Failed to insert document into MongoDB", "details": str(e)}), 500

            time.sleep(retry_interval)

        return jsonify({"error": "File not found in HDFS after retries"}), 404
    else:
        error_details = response.json()
        return jsonify({"error": "Failed to process request", "details": error_details}), response.status_code

def submit_wordcount_job1():
    data = request.json
    if 'display_value' not in data or 'input_file_ids' not in data:
        return jsonify({"error": "Missing parameters 'display_value' or 'input_file_ids'"}), 400

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('owner')
    input_file_ids = data['input_file_ids']
    parent_path = data.get('parent_path', f"/users/{owner}/analysis")

    hdfs_paths = resolve_hdfs_paths(input_file_ids)
    if not hdfs_paths:
        return jsonify({"error": "Invalid file IDs; HDFS paths not found"}), 400

    output_path = f"hdfs://Master1:9000/users/{owner}/analysis/{timestamp}_wordcount"
    k_value = str(data['display_value'])
    args = [k_value, output_path] + hdfs_paths

    return submit_spark_job1(
        "Word Count",
        "hdfs://Master1:9000/algorithms/new_wc.py",
        args,
        output_path,
        parent_path=parent_path,
        owner=owner,
        analysis_type="word_count"
    )

def submit_kmeans_job1():
    data = request.json
    if 'k_value' not in data or 'input_file_ids' not in data:
        return jsonify({"error": "Missing parameters 'k_value' or 'input_file_ids'"}), 400

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('owner')
    input_file_ids = data['input_file_ids']
    parent_path = data.get('parent_path', f"/users/{owner}")

    hdfs_paths = resolve_hdfs_paths(input_file_ids)
    if not hdfs_paths:
        return jsonify({"error": "Invalid file IDs; HDFS paths not found"}), 400

    output_path = f"hdfs://Master1:9000/users/{owner}/analysis/{timestamp}_kmeans"
    k_value = str(data['k_value'])
    args = [k_value, output_path] + hdfs_paths

    return submit_spark_job1(
        "K-Means",
        "hdfs://Master1:9000/algorithms/new_kmeans.py",
        args,
        output_path,
        parent_path=parent_path,
        owner=owner,
        analysis_type="kmeans"
    )

def submit_w2v_job1():
    data = request.json
    if 'w2v_param' not in data or 'input_file_ids' not in data:
        return jsonify({"error": "Missing parameters 'w2v_param' or 'input_file_ids'"}), 400

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('owner')
    input_file_ids = data['input_file_ids']
    parent_path = data.get('parent_path', f"/users/{owner}")

    hdfs_paths = resolve_hdfs_paths(input_file_ids)
    if not hdfs_paths:
        return jsonify({"error": "Invalid file IDs; HDFS paths not found"}), 400

    output_path = f"hdfs://Master1:9000/users/{owner}/analysis/{timestamp}_w2v"
    w2v_value = str(data['w2v_param'])
    args = [w2v_value, output_path] + hdfs_paths

    return submit_spark_job1(
        "Word2Vec",
        "hdfs://Master1:9000/algorithms/new_w2v.py",
        args,
        output_path,
        parent_path=parent_path,
        owner=owner,
        analysis_type="word2vec"
    )

def submit_tfidf_job1():
    data = request.json
    if 'tfidf_param' not in data or 'input_file_ids' not in data:
        return jsonify({"error": "Missing parameters 'tfidf_param' or 'input_file_ids'"}), 400

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('owner')
    input_file_ids = data['input_file_ids']
    parent_path = data.get('parent_path', f"/users/{owner}")

    hdfs_paths = resolve_hdfs_paths(input_file_ids)
    if not hdfs_paths:
        return jsonify({"error": "Invalid file IDs; HDFS paths not found"}), 400

    output_path = f"hdfs://Master1:9000/users/{owner}/analysis/{timestamp}_tfidf"
    tfidf_value = str(data['tfidf_param'])
    args = [tfidf_value, output_path] + hdfs_paths

    return submit_spark_job1(
        "TF-IDF",
        "hdfs://Master1:9000/algorithms/new_tfidf.py",
        args,
        output_path,
        parent_path=parent_path,
        owner=owner,
        analysis_type="tfidf"
    )

def submit_lda_job1():
    data = request.json
    if 'lda_param' not in data or 'input_file_ids' not in data:
        return jsonify({"error": "Missing parameters 'lda_param' or 'input_file_ids'"}), 400

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('owner')
    input_file_ids = data['input_file_ids']
    parent_path = data.get('parent_path', f"/users/{owner}")

    hdfs_paths = resolve_hdfs_paths(input_file_ids)
    if not hdfs_paths:
        return jsonify({"error": "Invalid file IDs; HDFS paths not found"}), 400

    output_path = f"hdfs://Master1:9000/users/{owner}/analysis/{timestamp}_lda"
    lda_value = str(data['lda_param'])
    args = [lda_value, output_path] + hdfs_paths

    return submit_spark_job1(
        "LDA",
        "hdfs://Master1:9000/algorithms/new_lda.py",
        args,
        output_path,
        parent_path=parent_path,
        owner=owner,
        analysis_type="lda"
    )

def submit_sma_job1():
    data = request.json
    if 'optionList' not in data or 'linkStrength' not in data or 'input_file_ids' not in data:
        return jsonify({"error": "Missing parameters 'optionList', 'linkStrength', or 'input_file_ids'"}), 400

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('owner')
    input_file_ids = data['input_file_ids']
    parent_path = data.get('parent_path', f"/users/{owner}")

    hdfs_paths = resolve_hdfs_paths(input_file_ids)
    if not hdfs_paths:
        return jsonify({"error": "Invalid file IDs; HDFS paths not found"}), 400

    output_path = f"hdfs://Master1:9000/users/{owner}/analysis/{timestamp}_sma"
    option_list = str(data['optionList'])
    link_strength = str(data['linkStrength'])
    args = [option_list, link_strength, output_path] + hdfs_paths

    return submit_spark_job1(
        "SMA",
        "hdfs://Master1:9000/algorithms/new_sma.py",
        args,
        output_path,
        parent_path=parent_path,
        owner=owner,
        analysis_type="sma"
    )

def submit_ngrams_job1():
    data = request.json
    if 'optionList' not in data or 'n' not in data or 'linkStrength' not in data or 'input_file_ids' not in data:
        return jsonify({"error": "Missing parameters 'optionList', 'n', 'linkStrength', or 'input_file_ids'"}), 400

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('owner')
    input_file_ids = data['input_file_ids']
    parent_path = data.get('parent_path', f"/users/{owner}")

    hdfs_paths = resolve_hdfs_paths(input_file_ids)
    if not hdfs_paths:
        return jsonify({"error": "Invalid file IDs; HDFS paths not found"}), 400

    output_path = f"hdfs://Master1:9000/users/{owner}/analysis/{timestamp}_ngrams"
    option_list = str(data['optionList'])
    n_value = str(data['n'])
    link_strength = str(data['linkStrength'])
    args = [option_list, n_value, link_strength, output_path] + hdfs_paths

    return submit_spark_job1(
        "N-Grams",
        "hdfs://Master1:9000/algorithms/new_ngrams.py",
        args,
        output_path,
        parent_path=parent_path,
        owner=owner,
        analysis_type="ngrams"
    )

def submit_hclustering_job1():
    data = request.json
    if 'input_file_ids' not in data:
        return jsonify({"error": "Missing parameters 'input_file_ids'"}), 400

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('owner')
    input_file_ids = data['input_file_ids']
    parent_path = data.get('parent_path', f"/users/{owner}")

    hdfs_paths = resolve_hdfs_paths(input_file_ids)
    if not hdfs_paths:
        return jsonify({"error": "Invalid file IDs; HDFS paths not found"}), 400

    output_path = f"hdfs://Master1:9000/users/{owner}/analysis/{timestamp}_hc"
    w2v_value = str(5)
    args = [w2v_value, output_path] + hdfs_paths

    return submit_spark_job1(
        "Hclustering",
        "hdfs://Master1:9000/algorithms/new_hc.py",
        args,
        output_path,
        parent_path=parent_path,
        owner=owner,
        analysis_type="hclustering"
    )

def submit_ner_job1():
    data = request.json
    if 'optionList' not in data or 'input_file_ids' not in data:
        return jsonify({"error": "Missing parameters 'optionList'"}), 400

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    owner = data.get('owner')
    input_file_ids = data['input_file_ids']
    parent_path = data.get('parent_path', f"/users/{owner}")

    hdfs_paths = resolve_hdfs_paths(input_file_ids)
    if not hdfs_paths:
        return jsonify({"error": "Invalid file IDs; HDFS paths not found"}), 400

    output_path = f"hdfs://Master1:9000/users/{owner}/analysis/{timestamp}_ner"
    entity = "NOUN,PER,ORG,LOC,GPE,PRODUCT"
    ner_value = str(data['ner_value'])
    args = [ner_value, entity, output_path] + hdfs_paths

    return submit_spark_job1(
        "NER",
        "hdfs://Master1:9000/algorithms/ner.py",
        args,
        output_path,
        parent_path=parent_path,
        owner=owner,
        analysis_type="ner"
    )

def get_analysis_result1():
    owner = request.args.get('owner')
    path = request.args.get('output_path')
    if not owner or not path:
        return jsonify({"error": "Owner and output path are required"}), 400

    result = webReadFile(path)
    if 'file_content' in result:
        print(result["file_content"])
        return Response(result['file_content'], mimetype='text/plain')
    else:
        return jsonify(result), 500

def get_status1(batch_id, owner):
    status_url = f"{LIVY_URL}/{batch_id}"
    response = requests.get(status_url, auth=AUTH, verify=False)
    return handle_response(response, owner=owner)

def get_log1(batch_id, owner):
    log_url = f"{LIVY_URL}/{batch_id}/log"
    response = requests.get(log_url, auth=AUTH, verify=False)
    return handle_response(response, owner=owner)
