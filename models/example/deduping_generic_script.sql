{{ config(materialized='table', alias= var('table') + "_test") }}

with 
input_data as (

{% if var('customBquery', None) is not none %}
    {% set input_string = var('customBquery') %}
    {% set modified_string = input_string.replace('^', ' ') %}

    {{ modified_string }}

{% else %}
    select {{ var('rows') }} from maplemonk.{{var('table')}}
{% endif %}

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
