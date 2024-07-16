{{ config(materialized='table', alias= var('table') + "_test") }}

with 
input_data as (

    SELECT t.*,JSON_EXTRACT_SCALAR(DEFAULT_ADDRESS, "$.id") AS created_0_1, JSON_EXTRACT_SCALAR(DEFAULT_ADDRESS, "$.customer_id") AS created_1_1 FROM maplemonk.CHECK__CUSTOMERS t, UNNEST(CASE WHEN t.DEFAULT_ADDRESS IS NOT NULL THEN JSON_EXTRACT_ARRAY(t.DEFAULT_ADDRESS) ELSE [] END) AS DEFAULT_ADDRESS

),
 new_updated as (
   SELECT * FROM (
        select *, row_number() over(
            partition by {{var("partitionRows")}}
            order by 
            {{var("cursor_feild")}} is null asc,
            {{var("cursor_feild")}} desc,
            _AIRBYTE_EMITTED_AT desc
        ) AS ROW_NUMBER
      FROM input_data
     ) WHERE ROW_NUMBER = 1
)
SELECT {{var("orignalField")}}  FROM new_updated
