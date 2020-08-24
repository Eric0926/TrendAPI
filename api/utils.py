import json
from google.cloud import spanner

client = spanner.Client("yiqing-twitter-candidates")
instance = client.instance("twitter-attack")
database = instance.database("twitter_db")


def fetch_stats(transaction):
    query = """
            SELECT * FROM six_hour_stat 
            WHERE time = TIMESTAMP("2020-08-23T06:00:00Z") AND toxic_num != 0 AND reply_num != 0
            """
    result = transaction.execute_sql(query)
    return list(result)


def fetch_candidates(transaction, tops_id):
    query = """
            SELECT * FROM candidate_table 
            WHERE candidate_id IN ({})
            """.format(tops_id)
    result = transaction.execute_sql(query)
    return list(result)


if __name__ == "__main__":
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
    for x in trends[:3]:
        print(x)
    trends_json = json.dumps(trends[:3])
    print(trends_json)
