import json
import math
from google.cloud import datastore
from datetime import datetime, timezone, timedelta
import pymysql

datastore_client = datastore.Client("yiqing-2020-twitter")


def fetch_last_hour_stats(time):
    db = pymysql.connect("35.202.99.165", "root", "twitter123", "twitter")
    cursor = db.cursor()
    sql = """
        SELECT a.candidate_id, a.reply, a.toxic_reply, a.opposing_party_toxic_reply, a.retweet, b.candidate_state, b.candidate_party, b.candidate_position, b.candidate_name, b.candidate_handle, b.candidate_followers_num
        FROM one_hour_stat a
        LEFT JOIN candidate_2020 b
        ON a.candidate_id = b.candidate_id
        WHERE a.commit_time=%s
    """
    time_str = "{}-{}-{} {}:00:00".format(time.year,
                                          time.month, time.day, time.hour)
    cursor.execute(sql, (time_str))
    results = list(cursor.fetchall())
    db.close()
    return results


def fetch_candidate_period_stats(candidate_id, start_time, end_time):
    db = pymysql.connect("35.202.99.165", "root", "twitter123", "twitter")
    cursor = db.cursor()
    sql = """
        SELECT * FROM one_hour_stat
        WHERE candidate_id=%s and commit_time>=%s and commit_time<%s
        ORDER BY candidate_id
    """
    start_time_str = "{}-{}-{} {}:00:00".format(start_time.year,
                                                start_time.month, start_time.day, start_time.hour)
    end_time_str = "{}-{}-{} {}:00:00".format(end_time.year,
                                              end_time.month, end_time.day, end_time.hour)
    cursor.execute(sql, (candidate_id, start_time_str, end_time_str))
    results = cursor.fetchall()
    db.close()
    return results


# candidate-2020 contains id, name followers_count, friends_count, handle, party, position
def fetch_candidate(candidate_id):
    db = pymysql.connect("35.202.99.165", "root", "twitter123", "twitter")
    cursor = db.cursor()
    sql = """
        SELECT * FROM candidate_2020
        WHERE candidate_id=%s
        ORDER BY candidate_id
    """
    cursor.execute(sql, (candidate_id))
    result = cursor.fetchone()
    db.close()
    return result


def generate_top20(results_top20):
    # candidate_id/reply/toxic/opposing/retweet/state/party/position/name/handle/followers_num/friends_num
    trends = []
    for r in results_top20:
        x = {}
        x["id"] = str(r[0])
        x["name"] = r[8]
        x["state"] = r[5]
        x["party"] = r[6]
        x["handle"] = r[9]
        x["position"] = r[7]
        x["followers_count"] = r[10]
        x["reply"] = r[1]
        x["retweet"] = r[4]
        x["toxic_reply"] = r[2]
        x["opposing"] = r[3]
        trends.append(x)
    return trends

# return: a list of top 10 candidates ascendingly by num_of_toxic / log(num_of_followers)
# id/time/reply/toxic_reply/opposing/retweet/


def last_hour_top20():
    tt = datetime.now(timezone.utc)
    lasthour = tt.hour - 1
    if lasthour == -1:
        lasthour = 23
    t = datetime(tt.year, tt.month, tt.day,
                 tt.hour, 0, 0, tzinfo=timezone.utc)if tt.minute >= 15 else datetime(tt.year, tt.month, tt.day,
                                                                                     lasthour, 0, 0, tzinfo=timezone.utc)
    # results contains: candidate_id/reply/toxic/opposing/retweet/state/party/position/name/handle/followers_num
    results = fetch_last_hour_stats(t)
    results.sort(
        key=lambda x: (-math.log(x[2] + 1)/(math.log(x[-2] + 1)+1)))
    top20 = generate_top20(results[:20])

    return top20


def last_n_days(candidate_id, n):

    candidate_info = fetch_candidate(candidate_id)
    info = {}
    info["id"] = candidate_id
    info["name"] = candidate_info[4]
    info["state"] = candidate_info[1]
    info["party"] = candidate_info[2]
    info["position"] = candidate_info[3]
    info["handle"] = candidate_info[5]
    info["followers_count"] = candidate_info[6]
    print(info)

    d = datetime.now(timezone.utc)
    end_time = datetime(d.year, d.month, d.day + 1,
                        0, 0, 0, tzinfo=timezone.utc)
    start_time = end_time - timedelta(days=n)
    results = fetch_candidate_period_stats(candidate_id, start_time, end_time)

    stats = []
    dateToIdx = {}
    for i in range(n, 0, -1):
        dd = end_time - timedelta(days=i)
        stat = {}
        stat["date"] = str(dd.date())
        stat["reply"] = 0
        stat["toxic_reply"] = 0
        stat["opposing"] = 0
        stat["retweet"] = 0
        stats.append(stat)
        dateToIdx[stat["date"]] = n - i

    data = {}
    data["examples"] = []
    data["example_urls"] = []

    # stat_id/candidate_id/commit_time/reply/toxic_reply/opposing_party_toxic_reply/retweet/tweet_ids/toxic_user_ids
    for r in results:
        # candidate_id, commit_time, reply, toxic_reply, opposing, retweet
        commit_date = str(r[2].date())
        stats[dateToIdx[commit_date]]["reply"] += r[3]
        stats[dateToIdx[commit_date]]["toxic_reply"] += r[4]
        stats[dateToIdx[commit_date]]["opposing"] += r[5]
        stats[dateToIdx[commit_date]]["retweet"] += r[6]

        # examples, example_urls
        # if r[6] is not None and len(data["examples"]) <= 10:
        #     data["examples"].extend(r[6])
        #     data["example_urls"].extend(r[7])

    for s in stats[:5]:
        print(s)

    # data["info"] = info
    # data["stats"] = stats

    # return data


if __name__ == "__main__":

    # print("Last Hour Stats")
    # top20 = last_hour_top20()
    # for r in top20:
    #     print(r)

    id = "138203134"
    last_n_days(id, 3)
    # end_time = datetime.now(timezone.utc)
    # start_time = end_time - timedelta(hours=10)
    # results = fetch_candidate_period_stats(id, start_time, end_time)
    # print(len(results))
    # for r in results:
    #     print(r[:-2])

    # id = "1249982359"
    # n = 10
    # print("Last {} Days for {}".format(n, id))
    # data = lastNDays(id, n)
    # print(data["info"])
    # for i in data["stats"]:
    #     print(i)
    # print(data["examples"])
    # print(data["example_urls"])
