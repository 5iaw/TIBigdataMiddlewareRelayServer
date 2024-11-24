# /services/spark.py
from flask import Flask, request, jsonify
import subprocess
import requests

def check_yarn_connection():
    """Check connection to the YARN ResourceManager."""
    try:
        response = requests.get('http://localhost:8088/cluster')
        if response.status_code == 200:
            return True, "YARN is reachable."
        else:
            return False, "YARN is not reachable."
    except requests.exceptions.RequestException as e:
        return False, str(e)

def check_connection():
    """Route to check YARN connection."""
    is_reachable, message = check_yarn_connection()
    if is_reachable:
        return jsonify({"message": message}), 200
    else:
        return jsonify({"error": message}), 503


def submit_job():
    """Route to submit a Spark job."""
    # Check connection to YARN
    is_reachable, message = check_yarn_connection()
    if not is_reachable:
        return jsonify({"error": message}), 503

    # Extract job parameters from the request
    job_script = request.json.get('job_script', None)

    if not job_script:
        return jsonify({"error": "Job script path is required."}), 400

    # Submit the Spark job
    try:
        result = subprocess.run(
            ['spark-submit', '--master', 'yarn', job_script],
            capture_output=True, text=True
        )

        if result.returncode == 0:
            return jsonify({"message": "Job submitted successfully!", "output": result.stdout}), 200
        else:
            return jsonify({"error": "Error submitting job", "stderr": result.stderr}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def submit_print_job():
    """Route to submit the print_save Spark job."""
    # Check connection to YARN
    is_reachable, message = check_yarn_connection()
    if not is_reachable:
        return jsonify({"error": message}), 503

    # Submit the Spark job
    try:
        result = subprocess.run(
            ['spark-submit', '--master', 'yarn', '--deploy-mode', 'cluster', './print.py'],
            capture_output=True, text=True
        )

        if result.returncode == 0:
            return jsonify({"message": "Job submitted successfully!", "output": result.stdout}), 200
        else:
            return jsonify({"error": "Error submitting job", "stderr": result.stderr}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500
