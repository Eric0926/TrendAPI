import json
import math
from google.cloud import spanner
from google.cloud import datastore
from google.cloud.datastore.key import Key
from datetime import datetime,timezone, timedelta

client = spanner.Client("yiqing-twitter-candidates")
instance = client.instance("twitter-attack")
database = instance.database("twitter_db")

def fetchLastWeekStats(transaction, start_time, end_time):
    # test = "2020-10-08T06:00:00Z"
    query = """
            SELECT * FROM one_hour_stat
            WHERE commit_time >= TIMESTAMP("{}")
            AND commit_time <= TIMESTAMP("{}")
            AND toxic_reply != 0 AND reply != 0
            """.format(start_time, end_time)
    result = transaction.execute_sql(query)
    return list(result)

# candidate-2020 contains id, name followers_count, friends_count, handle, party, position
def fetch_candidates(transaction, tops_id):
    query = """
            SELECT * FROM candidate_2020
            WHERE candidate_id IN ({})
            """.format(tops_id)
    result = transaction.execute_sql(query)
    return list(result)

def print_last_week_result(all_infor):
    new_candidate_table = []
    for i in range(len(all_info)):
        entry = []
        # id
        entry.append(all_info[i][0])
        # name
        entry.append(all_info[i][4])
        # followers_count
        entry.append(all_info[i][6])
        # reply
        entry.append(all[i][2])
        # retweet
        entry.append(all[i][5])
        # toxic
        entry.append(all[i][3])
        # opposing
        entry.append(all[i][4])
        # state
        entry.append(all_info[i][1])
        # party
        entry.append(all_info[i][2])
        new_candidate_table.append(entry)
    sorted_new_candidate_table = sorted(new_candidate_table, key= lambda result: (-math.log(result[5]+1)/math.log(result[2])+1))
    top10_in_last_hour = sorted_new_candidate_table[:10]
    top10_in_last_hour = list(reversed(top10_in_last_hour))
    # ascendingly allocate 10 candidate
    print("\n")
    print(sorted_new_candidate_table)
    print("\n")
    print(top10_in_last_hour)
    trends = []
    for i in range(10):
        x = {}
        x["id"] = top10_in_last_hour[i][0]
        x["name"] = top10_in_last_hour[i][1]
        x["state"] = top10_in_last_hour[i][7]
        x["party"] = top10_in_last_hour[i][8]
        x["followers_count"] = top10_in_last_hour[i][2]
        x["reply"] = top10_in_last_hour[i][3]
        x["retweet"] = top10_in_last_hour[i][4]
        x["toxic_reply"] = top10_in_last_hour[i][5]
        x["opposing"] = top10_in_last_hour[i][6]
        trends.append(x)
    for x in trends[:10]:
        print(x)
    print("In json: ")
    trends_json = json.dumps(trends[:10])
    print(trends_json)
if __name__ == "__main__":
    # get Time variable
    # create the api to interact with datastore
    #datastore_client = datastore.Client(PROJECT_ID_DATASTORE)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=7)
    results = database.run_in_transaction(fetchLastWeekStats, start_time, end_time)
    all = sorted(results, key=lambda x: x[0])
    # print(all)
    # print("\n")
    all_id = ",".join(str(x[0]) for x in all)
    all_info = database.run_in_transaction(fetch_candidates, all_id)
    # sort by log(toxic_reply) / log(followers_count)
    # tops = sorted(results_sorted[:10], key=lambda x: x[0])
    print_last_week_result(all_info)
