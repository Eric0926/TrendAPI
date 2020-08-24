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


if __name__ == "__main__":
    results = database.run_in_transaction(fetch_stats)
    results_sorted = sorted(
        results, key=lambda result: (-result[2], -result[3]))
    for result in results_sorted[:15]:
        print(result)
