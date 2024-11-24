import os
from elasticsearch import Elasticsearch
from flask import jsonify, request
from dotenv import load_dotenv

load_dotenv()

INDEX = "kubic_paper,kubic_news"

# Initialize the Elasticsearch client
es = Elasticsearch(
    [os.getenv('ELASTIC_HOST')],
    http_auth=(os.getenv('ELASTIC_ID'), os.getenv('ELASTIC_PASSWORD')),
    scheme="https",
    port=os.getenv('ELASTIC_PORT'),
    verify_certs=False  # Disable SSL certificate verification
)

# Test connection to Elasticsearch
def esTest():
    try:
        body = {
            "query": {
                "match_all": {}
            },
            "size": 1,
        }
        res = es.search(index=INDEX, body=body)
        print("Elasticsearch connection test response:", res)  # Log response for debugging
        return jsonify({'status': 'Elasticsearch is up and running', 'result': res}), 200
    except Exception as e:
        return jsonify({'status': 'Error', 'message': str(e)}), 500

# Perform the actual search query
def es_Query():
    try:
        data = request.json
        keyword = data.get("keyword", "")
        if not keyword:
            return jsonify({"error": "Keyword is required."}), 400

        query_body = {
            "query": {
                "multi_match": {
                    "query": keyword,
                    "fields": ["post_title", "post_body", "file_extracted_content"]
                }
            },
            "size": 50  # Adjust the number of results
        }

        # Execute the Elasticsearch query
        res = es.search(index=INDEX, body=query_body)

        # Extract relevant result data
        results = res.get("hits", {}).get("hits", [])
        formatted_results = [{"_id": hit["_id"], "source": hit["_source"]} for hit in results]

        return jsonify({"results": formatted_results})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

def es_query():
    try:
        data = request.json
        keyword = data.get("keyword", "")
        if not keyword:
            return jsonify({"error": "Keyword is required."}), 400

        query_body = {
            "query": {
                "multi_match": {
                    "query": keyword,
                    "fields": ["post_title", "post_body", "file_extracted_content"]
                }
            },
            "size": 200
        }

        res = es.search(index=INDEX, body=query_body)
        results = res.get("hits", {}).get("hits", [])
        formatted_results = [{"_id": hit["_id"], "source": hit["_source"]} for hit in results]

        return jsonify({"results": formatted_results})

    except Exception as e:
        # Log the error for debugging
        print(f"Error occurred: {str(e)}")
        return jsonify({"error": "Internal server error, check logs."}), 500
