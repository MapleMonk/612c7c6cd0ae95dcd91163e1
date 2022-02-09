
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table', alias= var('prefix')+ 'SPONSORED_DISPLAY_REPORT_STREAM') }}

with source_data as (
    select get_path(parse_json(_airbyte_data), '"metric"') as METRIC,
        to_varchar(get_path(parse_json(_airbyte_data), '"profileId"')) as PROFILEID,
        to_varchar(get_path(parse_json(_airbyte_data), '"recordType"')) as RECORDTYPE,
        to_varchar(get_path(parse_json(_airbyte_data), '"reportDate"')) as REPORTDATE,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
    from _AIRBYTE_RAW_{{var('prefix')}}SPONSORED_DISPLAY_REPORT_STREAM
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
