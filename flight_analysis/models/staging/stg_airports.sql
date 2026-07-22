with origins as (
    select distinct
        origin_airport_code     as airport_code,
        origin_city             as city
    from {{ ref('stg_flights') }}
),

destinations as (
    select distinct
        dest_airport_code       as airport_code,
        dest_city               as city
    from {{ ref('stg_flights') }}
),

combined as (
    select * from origins
    union
    select * from destinations
),

final as (
    select distinct
        airport_code,
        city
    from combined
)

select * from final