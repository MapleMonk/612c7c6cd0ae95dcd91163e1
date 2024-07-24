{{ config(materialized='table', alias= var('table') + "_test") }}

with 
input_data as (

{% if var('customBquery', None) is not none %}
    {% set input_string = var('customBquery') %}
    {% set modified_string = input_string.replace('^', ' ') %}

    {{ modified_string }}

{% else %}
    select {{ var('rows') }} from {{var('schema')}}.{{var('table')}}
{% endif %}

),
 new_updated as (
   SELECT * FROM 
   {% if var('partitionWithoutArrayObjects', None) is not none and var('partitionWithoutArrayObjects') != '' %}
    (
         SELECT *,row_number() over(
            partition by {{ var('partitionWithoutArrayObjects') }}
            order by 
            {{var("cursor_feild")}} is null asc,
            {{var("cursor_feild")}} desc,
            _AIRBYTE_EMITTED_AT desc
        ) AS ROW_NUMBER_1
        FROM 

   {% endif %}
   
   (
        select *, row_number() over(
            partition by {{var("partitionRows")}}
            order by 
            {{var("cursor_feild")}} is null asc,
            {{var("cursor_feild")}} desc,
            _AIRBYTE_EMITTED_AT desc
        ) AS ROW_NUMBER
      FROM input_data
     ) WHERE ROW_NUMBER = 1

    {% if var('partitionWithoutArrayObjects', None) is not none and var('partitionWithoutArrayObjects') != '' %}
       ) where ROW_NUMBER_1 = 1
   {% endif %}
   
)
SELECT {{var("orignalField")}}  FROM new_updated
