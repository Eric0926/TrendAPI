from google.cloud import spanner

client = spanner.clitn("yiqing-twitter-candidates")
instance = client.instance("twitter-attack")
database = instance.database("twitter_db")


def fetch_stats(transaction):
    query = """SELECT * FROM six_hout_stat"""
    result = transaction.execute_sql(query)
    return list(result)


if __name__ == "__main__":
    database.run_in_transaction(fetch_stats)