from flask import Flask, jsonify
from utils import *

app = Flask(__name__)


@app.route("/trends")
def trends():
    results = database.run_in_transaction(fetch_stats)
    results_sorted = sorted(
        results, key=lambda result: (-result[2], -result[3]))
    tops = sorted(results_sorted[:10], key=lambda x: x[0])
    tops_id = ",".join(str(x[0]) for x in tops)
    tops_info = database.run_in_transaction(fetch_candidates, tops_id)
    trends = []
    for i in range(10):
        x = {}
        x["name"] = tops_info[i][1]
        x["party"] = tops_info[i][2]
        x["state"] = tops_info[i][3] if tops_info[i][3] is not None else "none"
        x["twitterID"] = tops_info[i][4] if tops_info[i][4] is not None else "none"
        x["reply"] = tops[i][2]
        x["toxic"] = tops[i][3]
        x["retweet"] = tops[i][4]
        trends.append(x)
    trends_json = json.dumps(trends)
    return jsonify(trends), 200


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)
