
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

-- {{ config(materialized='table', alias= var('table') + "_test") }}

{{ config(materialized='table', alias = "tester_test") }}

-- with 
-- input_data as (

-- {% if var('customBquery', None) is not none %}
--     {{ var('customBquery') }}
-- {% else %}
--     select {{ var('rows') }} from {{var('table')}}
-- {% endif %}

-- ),
--  new_updated as (
--    SELECT * FROM (
--         select *, row_number() over(
--             partition by {{var("partitionRows")}}
--             order by 
--             {{var("cursor_feild")}} is null asc,
--             {{var("cursor_feild")}} desc,
--             _AIRBYTE_EMITTED_AT desc
--         ) AS ROW_NUMBER
--       FROM input_data
--      ) WHERE ROW_NUMBER = 1
-- )
-- SELECT {{var("orignalField")}}  FROM new_updated



with 
input_data as (

select * from maplemonk.TODELETE_CUSTOMERS

)
--  new_updated as (
--    SELECT * FROM (
--         select *, row_number() over(
--             partition by id
--             order by 
--             UPDATED_AT is null asc,
--             UPDATED_AT desc,
--             _AIRBYTE_EMITTED_AT desc
--         ) AS ROW_NUMBER
--       FROM input_data
--      ) WHERE ROW_NUMBER = 1
-- )
SELECT NOTE,ADDRESSES,LAST_ORDER_NAME  FROM input_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
