import os
from elasticsearch import Elasticsearch
from flask import jsonify, request
from dotenv import load_dotenv

load_dotenv()

# Initialize the Elasticsearch client
es = Elasticsearch(
    [os.getenv('ELASTIC_HOST')],
    http_auth=(os.getenv('ELASTIC_ID'), os.getenv('ELASTIC_PASSWORD')),
    scheme="https",
    port=os.getenv('ELASTIC_PORT'),
    verify_certs=False  # Disable SSL certificate verification
)

def ping():
    if es.ping():
        return jsonify({'status': 'Elasticsearch is online'}), 200
    else:
        return jsonify({'status': 'Elasticsearch is down'}), 500


def count():
    data = request.get_json()
    index = os.getenv('ELASTIC_INDEX')
    body = data.get('body')

    if not index or not body:
        return jsonify({'error': 'Index and body are required'}), 400

    try:
        result = es.count(index=index, body=body)
        return jsonify(result)
    except Exception as e:
        print(f'Error counting documents: {e}')
        return jsonify({'error': 'Error counting documents'}), 500
    
def search():
    data = request.get_json()
    index = (os.getenv('ELASTIC_INDEX')) 
    body = data.get('body')
    from_ = data.get('from', 0)
    size = data.get('size', None)
    filter_path = data.get('filterPath', None)
    _source_includes = data.get('_source', None)

    # Check for required parameters
    if not index:
        return jsonify({'error': 'Index is required'}), 400
    if not body:
        return jsonify({'error': 'Body is required'}), 400

    try:
        result = es.search(
            index=index,
            body=body,
            from_=from_,
            size=size,
            filter_path=filter_path,
            _source_includes=_source_includes
        )
        return jsonify(result)
    except Exception as e:
        print(f'Error searching documents: {e}')
        return jsonify({'error': f'Error searching documents: {str(e)}'}), 500

# Test connection to Elasticsearch
def esTest():
    try:
        body = {
            "query": {
                "match_all": {}
            },
            "size": 1,
        }
        res = es.search(index=os.getenv('ELASTIC_INDEX'), body=body)
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
        res = es.search(index=os.getenv('ELASTIC_INDEX'), body=query_body)

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

        res = es.search(index=os.getenv('ELASTIC_INDEX'), body=query_body)
        results = res.get("hits", {}).get("hits", [])
        formatted_results = [{"_id": hit["_id"], "source": hit["_source"]} for hit in results]

        return jsonify({"results": formatted_results})

    except Exception as e:
        # Log the error for debugging
        print(f"Error occurred: {str(e)}")
        return jsonify({"error": "Internal server error, check logs."}), 500

def id_query():
    try:
        # data = request.json
        # _id = data.get("_id")

        _id = "626b9e0fac7c4a9c7fbc5e51"
        if not _id:
            return jsonify({"error": "_id is required"}), 400

        # Fetch document by ID
        result = es.get(index=os.getenv('ELASTIC_INDEX'), id=_id, ignore=[404])

        if not result.get("found"):
            return jsonify({"error": f"Document with _id {_id} not found."}), 404

        # Return the document if found
        return jsonify({"_id": result["_id"], "source": result["_source"]})

    except Exception as e:
        print(f"Error occurred: {str(e)}")  # Log the error
        return jsonify({"error": "Internal server error"}), 500
