

{{ config(materialized='table') }}

with input_data as (
    select * from maplemonk.toDelete_customers
),
new_updated as (
    select 
        *,
        row_number() over(
            partition by id
            order by 
                UPDATED_AT is null asc,
                UPDATED_AT desc,
                _AIRBYTE_EMITTED_AT desc
        ) as row_number
    from input_data
)
select 
    NOTE,
    ADDRESSES,
    LAST_ORDER_NAME
from new_updated
where row_number = 1
