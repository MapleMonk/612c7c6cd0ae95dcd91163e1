{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create Or Replace Table maplemonk.KA_Intermediate_GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT as SELECT JSON_VALUE(_airbyte_data, \'$.asin\') AS asin, JSON_VALUE(_airbyte_data, \'$.startDate\') AS start_date, JSON_VALUE(_airbyte_data, \'$.endDate\') AS end_date, JSON_EXTRACT(_airbyte_data, \'$.clickData\') AS clickData, JSON_EXTRACT(_airbyte_data, \'$.cartAddData\') AS cartAddData, JSON_VALUE(_airbyte_data, \'$.dataEndTime\') AS dataEndTime, JSON_EXTRACT(_airbyte_data, \'$.purchaseData\') AS purchaseData, JSON_EXTRACT(_airbyte_data, \'$.impressionData\') AS impressionData, JSON_EXTRACT(_airbyte_data, \'$.searchQueryData\') AS searchQueryData, _airbyte_emitted_at FROM MapleMonk._airbyte_raw_KA_AmazonIndia_GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT qualify row_number() over(partition by JSON_VALUE(_airbyte_data, \'$.asin\'),JSON_VALUE(_airbyte_data, \'$.startDate\'),JSON_VALUE(_airbyte_data, \'$.searchQueryData.searchQuery\') order by _airbyte_emitted_at desc) = 1;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            