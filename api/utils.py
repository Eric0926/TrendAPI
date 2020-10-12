import json
import math
from google.cloud import spanner
from datetime import datetime, timezone, timedelta

client = spanner.Client("yiqing-twitter-candidates")
instance = client.instance("twitter-attack")
database = instance.database("twitter_db")


# return: a list of top 10 candidates ascendingly by num_of_toxic / log(num_of_followers)
# id/time/reply/toxic/opposing/retweet

# @params: time_id: datetime
def fetchLastHourStats(transaction, time_id):
    query = """
            SELECT * FROM one_hour_stat
            WHERE commit_time = TIMESTAMP("{}")
            """.format(time_id)
    result = transaction.execute_sql(query)
    return list(result)


def sampleFetchLastHour():
    tt = datetime.now(timezone.utc)

    t = datetime(tt.year, tt.month, tt.day, tt.hour, 0,
                 0, tzinfo=timezone.utc) if t.minute >= 15 else datetime(tt.year, tt.month, tt.day, tt.hour - 1, 0,
                                                                         0, tzinfo=timezone.utc)

    results = spanner_database.run_in_transaction(fetchLastHourStats, t)

    print(len(results))
    print(results[:5])


def fetchLastWeekStats(transaction):
    query = """
            SELECT * FROM one_hour_stat
            WHERE commit_time = TIMESTAMP("2020-10-08T06:00:00Z") AND toxic_reply != 0 AND reply != 0
            """
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

# method 2


def updates(transaction, time_id):
    query = """
            SELECT * FROM one_hour_stat
            WHERE commit_time = @time_id
            AND toxic_reply != 0 AND reply != 0
            """
    result = transaction.execute_sql(
        query,
        params={'time_id': time_id},
        param_types={'time_id': spanner.param_types.STRING}
    )
    return list(result)


# * last hour reply num 
# * last hour retweet num 
# * last hour toxic 
# * last hour opposing 
# * 1-week reply 
# * 1-week retweet 
# * 1-week toxic 
# * 1-week opposing
if __name__ == "__main__":
    # get Time variable
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)
    tt = end_time
    time = datetime(tt.year, tt.month, tt.day,
                    tt.hour, tt.minute, tt.second)
    string = datetime.utcnow().isoformat()
    print(string)
    time_id = string[0:19]+'Z'
    print(time_id)
    time_id.strip()
    x = list(time_id)
    print(x)
    # last hour table contains: # id/time/reply/toxic/opposing/retweet
    test_method2 = database.run_in_transaction(updates, time_id)
    print(test_method2)
    # method 1
    results = database.run_in_transaction(fetchLastHourStats, time_id)
    all = sorted(results, key=lambda x: x[0])
    print(all)
    print("\n")
    all_id = ",".join(str(x[0]) for x in all)
    all_info = database.run_in_transaction(fetch_candidates, all_id)
    # sort by log(toxic_reply) / log(followers_count)
    # tops = sorted(results_sorted[:10], key=lambda x: x[0])
    print(all_info)
    new_candidate_table = []
    print("\n")
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
        new_candidate_table.append(entry)
    sorted_new_candidate_table = sorted(
        new_candidate_table, key=lambda result: (-math.log(result[5])/math.log(result[2])))
    top10 = sorted_new_candidate_table[:10]
    top10 = list(reversed(top10))
    # ascendingly allocate 10 candidate
    print("\n")
    print(sorted_new_candidate_table)
    print("\n")
    print(top10)
    # print(tops)
    # print('\n')
    # print(tops)
    # tops_id = ",".join(str(x[0]) for x in tops)
    # # print("\n")
    # # print(tops_id)
    # tops_info = database.run_in_transaction(fetch_candidates, tops_id)
    # # print("\n")
    # # print(tops_info)
    # trends = []
    # for i in range(10):
    #     x = {}
    #     x["name"] = tops_info[i][1]
    #     x["party"] = tops_info[i][2]
    #     x["state"] = tops_info[i][3] if tops_info[i][3] is not None else "none"
    #     x["twitterID"] = tops_info[i][4] if tops_info[i][4] is not None else "none"
    #     x["reply"] = tops[i][2]
    #     x["toxic"] = tops[i][3]
    #     x["retweet"] = tops[i][4]
    #     trends.append(x)
    # for x in trends[:3]:
    #     print(x)
    # trends_json = json.dumps(trends[:3])
    # print(trends_json)
