with airlines as (
    select * from {{ ref('airlines') }}
)

select
    airline_code,
    airline_name
from airlines