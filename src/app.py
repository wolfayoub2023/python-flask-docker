from flask import Flask, request, jsonify
import threading
import time
import requests
import random

app = Flask(__name__)

# In-memory storage for task progress
tasks = {}

# NocoDB API details (without API token)
NOCODB_BASE_URL = "https://tables.reachpulse.co"

# Function to simulate bulk data insertion in batches and interact with NocoDB
def insert_in_batches(task_id, orgs, baseName, tableName, bulkRows, api_token):
    total_rows = len(bulkRows)
    batch_size = 10  # Adjust batch size as needed
    retries = 3  # Number of retries for failed requests
    retry_delay = 2  # Delay before retrying (seconds)

    try:
        for i in range(0, total_rows, batch_size):
            batch = bulkRows[i:i + batch_size]
            attempt = 0
            success = False
            while attempt < retries:
                try:
                    # Send batch to NocoDB API
                    response = requests.post(
                        f"{NOCODB_BASE_URL}/api/v1/db/data/bulk/{orgs}/{baseName}/{tableName}",
                        headers={
                            'Content-Type': 'application/json',
                            'xc-token': api_token  # Use the token from request headers
                        },
                        json=batch,
                        verify=False
                    )
                    
                    # Check if response status is 200, otherwise raise an error
                    if response.status_code == 200:
                        success = True
                        break
                    else:
                        raise Exception(f"NocoDB API returned {response.status_code}: {response.text}")
                except requests.exceptions.RequestException as e:
                    attempt += 1
                    time.sleep(random.randint(retry_delay, retry_delay + 2))  # Exponential backoff approach
                    tasks[task_id]['status'] = f"Retrying... Attempt {attempt}/{retries}"
                    tasks[task_id]['progress'] = int((i / total_rows) * 100)
                    if attempt == retries:
                        tasks[task_id]['status'] = f"Error during insertion: {str(e)}"
                        tasks[task_id]['progress'] = 0
                        return
            
            if not success:
                break  # Exit the loop if all retries failed

            # Simulate processing time for each batch
            time.sleep(1)

            # Update task progress
            progress = min(int((i + batch_size) / total_rows * 100), 100)
            tasks[task_id]['progress'] = progress

        # Finalize task as complete
        tasks[task_id]['status'] = 'Completed'
    except Exception as e:
        tasks[task_id]['status'] = f"Error during insertion: {str(e)}"
        tasks[task_id]['progress'] = 0
        return

# 1. Endpoint to start the bulk insertion and return a task ID
@app.route('/insert_bulk', methods=['POST'])
def insert_bulk():
    try:
        data = request.json
        orgs = data.get('orgs')
        baseName = data.get('baseName')
        tableName = data.get('tableName')
        bulkRows = data.get('bulkRows')

        # Get API token from headers
        api_token = request.headers.get('xc-token')

        # Validate required parameters
        if not all([orgs, baseName, tableName, bulkRows, api_token]):
            return jsonify({"error": "Missing required parameters or API token"}), 400

        task_id = str(int(time.time()))  # Use current timestamp as a task ID
        tasks[task_id] = {'status': 'In Progress', 'progress': 0, 'totalemails': len(bulkRows)}

        # Start a background thread for inserting in batches
        threading.Thread(target=insert_in_batches, args=(task_id, orgs, baseName, tableName, bulkRows, api_token)).start()

        return jsonify({
            "taskid": task_id,
            "status": "In Progress",
            "totalemails": len(bulkRows),
            "debug": "Bulk insert started successfully. Check progress with task ID."
        }), 200
    except Exception as e:
        return jsonify({"error": f"Exception occurred: {str(e)}"}), 500

# 2. Endpoint to check the progress of the insertion based on task ID
@app.route('/check_progress/<task_id>', methods=['GET'])
def check_progress(task_id):
    try:
        task = tasks.get(task_id)
        if not task:
            return jsonify({"error": "Task not found"}), 404

        return jsonify({
            "taskid": task_id,
            "progress": task['progress'],
            "status": task['status'],
            "totalemails": task.get('totalemails', 0),
            "debug": "Progress checked successfully."
        }), 200

    except Exception as e:
        return jsonify({"error": f"Exception occurred: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True)
