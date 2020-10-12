from datetime import datetime, timezone, timedelta
end_time = datetime.now(timezone.utc)
print(end_time)
start_time = end_time - timedelta(hours=1)
tt = end_time
time = datetime(tt.year, tt.month, tt.day,
                   tt.hour,tt.minute, tt.second)
print(time)

time_string = str(time)[0:10] + "T" + str(time)[11:19] + "Z"
string = datetime.utcnow().isoformat()
print(string)
substring  = string[0:19]+"Z"
print(substring)
query = """
        SELECT * FROM one_hour_stat
        WHERE time_string = TIMESTAMP(“{}”)AND toxic_reply != 0 AND reply != 0
        """.format(time_string)
print(query)


"2020-10-08T06:00:00Z"
