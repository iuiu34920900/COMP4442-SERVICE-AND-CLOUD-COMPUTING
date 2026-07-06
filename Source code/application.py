from flask import Flask, render_template, request, jsonify
import json
from pathlib import Path

application = Flask(__name__)

OUTPUT_DIR = Path("output")
SUMMARY_PATH = OUTPUT_DIR / "summary"
CHART_DATA_PATH = OUTPUT_DIR / "chartData"
summary_data = []

@application.route("/")
def index():
    return render_template('index.html')

@application.route("/monitor")
def monitor():
    driver_id = request.args.get('driver', '')
    return render_template('monitor.html', driver_id=driver_id)

@application.route("/getDriver_data/<driver_id>")
def get_driver_data(driver_id):
    for d in summary_data:
        if d['driverID'] == driver_id:
            return jsonify(d)
    return jsonify({'Data '+driver_id+' not found'}), 404

@application.route("/getDriver_chart/<driver_id>")
def get_driver_chart_data(driver_id):
    driver_path = CHART_DATA_PATH / f"driverID={driver_id}"
    if driver_path.exists():
        chart_json = list(driver_path.glob("part-*.json"))
        driver_data = []
        if chart_json:
            with open(chart_json[0], 'r', encoding='utf-8') as file:
                for l in file:
                    driver_data.append(json.loads(l))
        return jsonify(driver_data)
    return jsonify([])

@application.route('/getSummary')
def get_summary():
    return jsonify(summary_data)

def load_summary_data():
    global summary_data
    summary_json = list(SUMMARY_PATH.glob("part-*.json"))

    if summary_json:
        with open(summary_json[0], 'r', encoding='utf-8') as file:
            for l in file:
                res = json.loads(l)
                summary_data.append(res)
    else:
        print("err_summary_json")

load_summary_data()

if __name__ == "__main__":
    application.run(port=5000, debug=True)