with source as (
    select * from {{ source('main', 'raw_flights') }}
),

renamed as (
    select
        -- date/time
        fl_date                             as flight_date,
        year,
        quarter,
        month,
        day_of_month,
        day_of_week,

        -- airline/flight
        op_unique_carrier                   as airline_code,
        op_carrier_fl_num                   as flight_number,

        -- origin
        origin                              as origin_airport_code,
        origin_city_name                    as origin_city,
        origin_state_abr                    as origin_state,

        -- destination
        dest                                as dest_airport_code,
        dest_city_name                      as dest_city,
        dest_state_nm                       as dest_state,

        -- departure performance
        dep_time                            as departure_time,
        dep_delay                           as departure_delay_minutes,
        dep_del15                           as is_departure_delayed_15,

        -- arrival performance
        arr_time                            as arrival_time,
        arr_delay                           as arrival_delay_minutes,
        arr_del15                           as is_arrival_delayed_15,

        -- cancellations/diversions
        cancelled                           as is_cancelled,
        cancellation_code,
        diverted                            as is_diverted,

        -- flight details
        air_time                            as air_time_minutes,
        distance                            as distance_miles

    from source
)

select * from renamed