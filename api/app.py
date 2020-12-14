from flask import Flask, request, jsonify
from utils import *

app = Flask(__name__)


@app.route("/fetchLastHour", methods=["GET"])
def fetch_last_hour():
    trends = last_hour_top20()
    data = {"stats": trends}
    return jsonify(data), 200


@app.route("/fetchLastNDays", methods=["POST"])
def fetch_last_n_days():

    if ("candidate_id" not in request.json) or ("n_days" not in request.json):
        return "Parameters Missing", 400

    candidate_id = request.json["candidate_id"]
    n_days = int(request.json["n_days"])
    data = last_n_days(candidate_id, n_days)
    return jsonify(data)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)
    # app.run()
