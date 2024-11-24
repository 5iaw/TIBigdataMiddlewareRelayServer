# utils/es_utils.py

from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os

load_dotenv()

INDEX = "kubic_paper,kubic_news"

# Elasticsearch connection
es = Elasticsearch(
    [os.getenv('ELASTIC_HOST')],
    http_auth=(os.getenv('ELASTIC_ID'), os.getenv('ELASTIC_PASSWORD')),
    scheme="https",
    port=os.getenv('ELASTIC_PORT'),
    verify_certs=False
)

def getESQueryByID(id):
    query_body = {
        "query": {
            "multi_match": {
                "query": id,
                "fields": ["_id"]
            }
        },
        "size": 10  # Adjust the number of results returned as needed
    }

    # Execute the search query
    res = es.search(index=INDEX, body=query_body)

    # Extract and return the results
    results = res.get("hits", {}).get("hits", [])
    formatted_results = [{"_id": hit["_id"], "source": hit["_source"]} for hit in results]

    return formatted_results
