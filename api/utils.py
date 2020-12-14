import json
import math
from google.cloud import spanner
from datetime import datetime, timezone, timedelta
import pymysql

client = spanner.Client("yiqing-twitter-candidates")
instance = client.instance("twitter-attack")
database = instance.database("twitter_db")

# * last hour reply num
# * last hour retweet num
# * last hour toxic
# * last hour opposing
# * 1-week reply
# * 1-week retweet
# * 1-week toxic
# * 1-week opposing


def fetch_last_hour_stats(time):
    db = pymysql.connect("35.202.99.165", "root", "twitter123", "twitter")
    cursor = db.cursor()
    sql = """
        SELECT * FROM one_hour_stat
        WHERE commit_time=%s
    """
    time_str = "{}-{}-{} {}:00:00".format(time.year,
                                          time.month, time.day, time.hour)
    cursor.execute(sql, (time_str))
    results = cursor.fetchall()
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
def fetch_candidates(candidate_ids):
    db = pymysql.connect("35.202.99.165", "root", "twitter123", "twitter")
    cursor = db.cursor()
    sql = """
        SELECT * FROM candidate_2020
        WHERE candidate_id IN ({})
        ORDER BY candidate_id
    """.format(candidate_ids)
    cursor.execute(sql)
    results = cursor.fetchall()
    db.close()
    return results


def process_new_candidate_table(all_info, all):
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
        # handle
        entry.append(all_info[i][5])
        # position
        entry.append(all_info[i][3])
        new_candidate_table.append(entry)
    sorted_new_candidate_table = sorted(
        new_candidate_table, key=lambda result: (
            -math.log(result[5] + 1)/(math.log(result[2] + 1)+1)))
    return sorted_new_candidate_table


def generate_the_trend(top10_trend_table):
    trends = []
    for i in range(10):
        x = {}
        x["id"] = str(top10_trend_table[i][0])
        x["name"] = top10_trend_table[i][1]
        x["state"] = top10_trend_table[i][7]
        x["party"] = top10_trend_table[i][8]
        x["handle"] = top10_trend_table[i][9]
        x["position"] = top10_trend_table[i][10]
        x["followers_count"] = top10_trend_table[i][2]
        x["reply"] = top10_trend_table[i][3]
        x["retweet"] = top10_trend_table[i][4]
        x["toxic_reply"] = top10_trend_table[i][5]
        x["opposing"] = top10_trend_table[i][6]
        trends.append(x)
    return trends

# return: a list of top 10 candidates ascendingly by num_of_toxic / log(num_of_followers)
# id/time/reply/toxic_reply/opposing/retweet/


def last_hour():
    # last_hour_stat calculation
    tt = datetime.now(timezone.utc)
    lasthour = tt.hour - 1
    if lasthour == -1:
        lasthour = 23
    t = datetime(tt.year, tt.month, tt.day,
                 tt.hour, 0, 0, tzinfo=timezone.utc)if tt.minute >= 15 else datetime(tt.year, tt.month, tt.day,
                                                                                     lasthour, 0, 0, tzinfo=timezone.utc)
    # results contains: entry_id/candidate_id/time/reply/toxic/opposing/retweet/tweet_ids/toxic_user_ids
    results = fetch_last_hour_stats(t)
    candidate_ids = ",".join(str(x[1]) for x in results)
    if candidate_ids == "":
        return []
    candidate_infos = fetch_candidates(candidate_ids)
    print(len(results))
    print(len(candidate_infos))
    for info, r in list(zip(candidate_infos, results))[:5]:
        print(info)
        print(r[1:-2])
        print()

    # sorted_last_hour_table = process_new_candidate_table(
    #     all_info_last_hour, all_last_hour)
    # top10_in_last_hour = sorted_last_hour_table[:10]
    # trends_in_last_hour = generate_the_trend(top10_in_last_hour)

    # return trends_in_last_hour


def lastNDays(candidate_id, n):

    candidate_info = database.run_in_transaction(
        fetch_candidates, candidate_id)[0]
    info = {}
    info["id"] = candidate_id
    info["name"] = candidate_info[4]
    info["state"] = candidate_info[1]
    info["party"] = candidate_info[2]
    info["position"] = candidate_info[3]
    info["handle"] = candidate_info[5]
    info["followers_count"] = candidate_info[6]

    d = datetime.now(timezone.utc)
    end_time = datetime(d.year, d.month, d.day + 1,
                        0, 0, 0, tzinfo=timezone.utc)
    start_time = end_time - timedelta(days=n)
    results = database.run_in_transaction(
        fetchCandidatePeriodStats, candidate_id, start_time, end_time)

    stats = []
    dateToIdx = {}
    for i in range(n, 0, -1):
        dd = end_time - timedelta(days=i)
        stat = {}
        stat["date"] = str(dd.date())
        dateToIdx[stat["date"]] = n - i
        stat["reply"] = 0
        stat["toxic_reply"] = 0
        stat["opposing"] = 0
        stat["retweet"] = 0
        stats.append(stat)

    data = {}
    data["examples"] = []
    data["example_urls"] = []

    for r in results:
        # candidate_id, commit_time, reply, toxic_reply, opposing, retweet
        commit_date = str(r[1].date())
        stats[dateToIdx[commit_date]]["reply"] += r[2]
        stats[dateToIdx[commit_date]]["toxic_reply"] += r[3]
        stats[dateToIdx[commit_date]]["opposing"] += r[4]
        stats[dateToIdx[commit_date]]["retweet"] += r[5]

        # examples, example_urls
        if r[6] is not None and len(data["examples"]) <= 10:
            data["examples"].extend(r[6])
            data["example_urls"].extend(r[7])

    data["info"] = info
    data["stats"] = stats

    return data


if __name__ == "__main__":

    # print("Last Hour Stats")
    last_hour()
    # trends = lastHour()
    # for i in trends:
    #     print(i)
    # print("\n")

    # id = "138203134"
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
