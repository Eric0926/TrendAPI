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


def fetch_candidates(transaction):
    query = """
            SELECT * FROM candidate_table 
            WHERE candidate_id IN (138203134, 15764644, 818948638890217473, 29501253, 13218102, 942156122, 18166778, 482450423, 21059255, 1079104563280527364)
            """
    result = transaction.execute_sql(query)
    return list(result)


if __name__ == "__main__":
    results = database.run_in_transaction(fetch_stats)
    results_sorted = sorted(
        results, key=lambda result: (-result[2], -result[3]))
    tops = sorted(results_sorted[:10], key=lambda x: x[0])
    print([x[0] for x in tops])
    tops_info = database.run_in_transaction(fetch_candidates)
    print(tops_info)
