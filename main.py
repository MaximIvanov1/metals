import os
import json

from flask import Flask, request, jsonify, send_from_directory, abort 
from datetime import datetime


from job import run_extraction_job, run_extraction_job_all 

app = Flask(__name__)

RAW_DATA_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'raw')
os.makedirs(RAW_DATA_ROOT, exist_ok=True) 
-

@app.route('/', methods=['GET'])
def server_status():
    """Повертає статус, щоб підтвердити, що сервер працює."""
    return jsonify({
        "status": "Flask API Service is running",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "job_endpoints": [
            "/extract_metals_data (POST method, requires JSON body)",
            "/extract_all_data (POST method, no JSON body required)",
            "/data/raw/<feature>/<date> (GET method, retrieves saved JSON)"
        ]
    }), 200

@app.route('/extract_metals_data', methods=['POST'])
def extract_metals_data():
    """Обробляє POST-запити для запуску Job-функції з параметрами."""
    
    if not request.is_json:
        return jsonify({"status": "error", "message": "Missing JSON in request"}), 400

    data = request.get_json()
    
    required_keys = ['date', 'feature']
    if not all(key in data for key in required_keys):
        return jsonify({"status": "error", "message": "Missing required keys: 'date' and 'feature'"}), 400

    date = data.get('date')
    feature = data.get('feature')
         
    try:
        output_path = run_extraction_job(
            date=date, 
            feature=feature, 
            raw_dir=RAW_DATA_ROOT
        )
        
        if output_path:
            return jsonify({
                "status": "success",
                "message": f"Data extraction job completed successfully.",
                "feature": feature,
                "date": date,
                "saved_to": output_path
            }), 200
        else:
            return jsonify({
                "status": "error", 
                "message": "Data extraction job failed or returned no valid path. Check server logs."
            }), 500
            
    except Exception as e:
        app.logger.error(f"Critical error during job execution: {e}")
        return jsonify({"status": "error", "message": f"Internal server error: {str(e)}"}), 500

@app.route('/extract_all_data', methods=['POST'])
def extract_all_data():
    """Запускає Job для вивантаження всіх історичних даних."""
    
    try:
        # Виклик Job 2
        output_files = run_extraction_job_all(
            raw_dir=RAW_DATA_ROOT
        )
        
        if output_files is not None:
            return jsonify({
                "status": "success",
                "message": f"Successfully extracted and saved {len(output_files)} files.",
                "total_files": len(output_files),
                "saved_to_dir": os.path.join(RAW_DATA_ROOT, "ALL_HISTORICAL_DATA")
            }), 200
        else:
            return jsonify({
                "status": "error", 
                "message": "Global data extraction job failed. Check server logs."
            }), 500
            
    except Exception as e:
        app.logger.error(f"Critical error during global job execution: {e}")
        return jsonify({"status": "error", "message": f"Internal server error: {str(e)}"}), 500

@app.route('/data/raw/<feature>/<date>', methods=['GET'])
def get_raw_data(feature, date):
    """Повертає вміст збереженого JSON-файлу."""
    target_feature = feature.upper()
    
    if target_feature == "ALL_HISTORICAL_DATA":
        return jsonify({"error": "Direct access to ALL_HISTORICAL_DATA root is not supported via this simple route. Use /data/raw/<FEATURE>/<DATE>."}), 400

    target_dir = os.path.join(RAW_DATA_ROOT, target_feature, date)
    file_name = f"{date}.json"
    target_path = os.path.join(target_dir, file_name)

    if not os.path.exists(target_path):
        # Перевіряємо також загальну структуру ALL_HISTORICAL_DATA/{FEATURE}/{DATE}.json
        target_dir_all = os.path.join(RAW_DATA_ROOT, "ALL_HISTORICAL_DATA", target_feature)
        target_path_all = os.path.join(target_dir_all, file_name)
        
        if os.path.exists(target_path_all):
            return send_from_directory(target_dir_all, file_name, as_attachment=False, mimetype='application/json')
        
        return jsonify({"error": f"File not found for feature '{feature}' on date '{date}'"}), 404

    try:
        # as_attachment=False для відображення JSON у браузері
        return send_from_directory(target_dir, file_name, as_attachment=False, mimetype='application/json')
    except Exception as e:
        app.logger.error(f"Error serving file: {e}")
        abort(500)


if __name__ == '__main__':
    print(f"Flask server running locally on http://127.0.0.1:8081/")
    app.run(host='127.0.0.1', port=8081, debug=True)
