select 
        period,
        count(*) as num_rides,
        round(avg(trip_duration), 2) as avg_trip_duration,
        round(avg(trip_distance), 2) as avg_trip_distance,
        round(sum(trip_distance), 2) as total_trip_distance,
        round(avg(total_amount), 2) as avg_trip_price,
        round(sum(total_amount), 2) as total_trip_price,
        round(avg(tip_amount), 2) as avg_tip_amount
    from (
        select
            date_part('year', tpep_pickup_datetime) as trip_year,
            strftime(tpep_pickup_datetime, '%Y-%m') as period,
            epoch(tpep_dropoff_datetime - tpep_pickup_datetime) as trip_duration,
            trip_distance,
            total_amount,
            tip_amount
        from parquet_scan('nyc_yellow_cab_data_staging/**/*.parquet')
        where trip_year >= 2021 and trip_year <= 2024
    )
    group by period
    order by period;