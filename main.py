import os
import sys

import polars as pl
import time

data_path = "nyc_yellow_cab_data_staging/**/*.parquet"
zone_path = "./taxi_zone_lookup.csv"

query_map = {
    "query_1": f"select count(*) from nyc_yellow_city_data;",
    "query_2": f"select count(*),sum(total_amount),extract(YEAR FROM  tpep_pickup_datetime), extract(MONTH FROM  tpep_pickup_datetime) from nyc_yellow_city_data group by extract(YEAR FROM  tpep_pickup_datetime) ,extract(month FROM  tpep_pickup_datetime);",
    "query_3": f"select count(*),sum(total_amount),extract(year FROM  tpep_pickup_datetime) as year from nyc_yellow_city_data group by year;",
    "query_4": f"select count(*),vendorid,extract(year FROM  tpep_pickup_datetime) as year, extract(month FROM  tpep_pickup_datetime) as month from nyc_yellow_city_data group by year,month,vendorid;",
    "query_5": f"select count(*),vendorid,extract(year FROM  tpep_pickup_datetime) as year  from nyc_yellow_city_data group by year,vendorid;",
    "query_6": f"select count(*) as total_rides,PULocationID,extract(year FROM  tpep_pickup_datetime) as year, extract(month FROM  tpep_pickup_datetime) as month from nyc_yellow_city_data group by year,month,PULocationID",
    "query_7": f"select count(*) as total_rides,PULocationID from nyc_yellow_city_data group by PULocationID order by total_rides limit 10;",

    "query_8": f"select count(*) as total_rides,DOLocationID,extract(year FROM  tpep_pickup_datetime) as year, extract(month FROM  tpep_pickup_datetime) as month from nyc_yellow_city_data group by year,month,DOLocationID",
    "query_9": f"select count(*) as total_rides,DOLocationID from nyc_yellow_city_data group by DOLocationID order by total_rides limit 10;",

    "query_10": f"SELECT extract(year FROM tpep_pickup_datetime) AS year, extract(month FROM tpep_pickup_datetime) AS month, AVG(trip_distance) AS avg_trip_distance FROM nyc_yellow_city_data  GROUP BY year, month ORDER BY year, month;",
    "query_11": f"SELECT extract(year FROM tpep_pickup_datetime) AS year, AVG(trip_distance) AS avg_trip_distance FROM nyc_yellow_city_data  GROUP BY year  ORDER BY year;",

    "query_12": f"SELECT passenger_count, avg(total_amount) FROM nyc_yellow_city_data GROUP BY passenger_count",
    "query_13": f"SELECT passenger_count, extract(year FROM tpep_pickup_datetime) AS year, count(*) FROM nyc_yellow_city_data GROUP BY passenger_count, year",
    "query_14": f"SELECT passenger_count, extract(year FROM tpep_pickup_datetime) AS year, ceil(trip_distance) AS distance, count(*) FROM nyc_yellow_city_data GROUP BY passenger_count, year, distance ORDER BY year, count(*) DESC",
    "query_15": f"SELECT tz.zone AS zone, count(*) AS c FROM td LEFT JOIN trips_zone tz ON td.PULocationID = tz.locationid GROUP BY zone ORDER BY c DESC",
    "query_16": f"select period, count(*) as num_rides, round(avg(trip_duration), 2) as avg_trip_duration, round(avg(trip_distance), 2) as avg_trip_distance, round(sum(trip_distance), 2) as total_trip_distance, round(avg(total_amount), 2) as avg_trip_price, round(sum(total_amount), 2) as total_trip_price, round(avg(tip_amount), 2) as avg_tip_amount from ( select date_part('year', tpep_pickup_datetime) as trip_year, strftime(tpep_pickup_datetime, '%Y-%m') as period, epoch(tpep_dropoff_datetime - tpep_pickup_datetime) as trip_duration, trip_distance, total_amount, tip_amount from where trip_year >= 2014 and trip_year < 2024 ) group by period order by period",
}

if __name__ == "__main__":
    query_id = sys.argv[1]
    df = pl.scan_parquet(data_path)
    tzdf = pl.scan_csv(zone_path)
    # Register the DataFrame as a temporary table
    ctx = pl.SQLContext(register_globals=True, eager_execution=False)
    ctx.register("nyc_yellow_city_data", df)
    ctx.register("trips_zone", tzdf)
    query = query_map[query_id]
    print(query)
    start = time.time_ns()
    sql_result = ctx.execute(query)
    sql_result_collected = sql_result.collect()
    print(sql_result_collected)
    end = time.time_ns()
    query_time_ms = (end - start) / 1e6

    file_path = "results_polar.csv"
    if not os.path.exists(file_path):
        with open(file_path, 'w') as file:
            file.write("query_id,engine,query_time_ms")
    with open(file_path, 'a') as fd:
        fd.write(f'\n{query_id},polars,{query_time_ms}')