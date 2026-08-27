{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE VIEW SNITCH_DB.MAPLEMONK.V_META_PRODUCT_SPENDS_CATALOGUE AS SELECT TRY_TO_DATE(_airbyte_data:day::STRING) AS spend_date, _airbyte_data:sku_id::STRING AS sku_id, _airbyte_data:sku_with_size::STRING AS sku_with_size, _airbyte_data:category::STRING AS category, TRIM(SPLIT_PART(_airbyte_data:product_id::STRING, \',\', 1)) AS product_code, TRIM(SUBSTR(_airbyte_data:product_id::STRING, POSITION(\',\' IN _airbyte_data:product_id::STRING) + 1)) AS product_title, TRY_CAST(NULLIF(_airbyte_data:amount_spent_inr::STRING,\'\') AS NUMBER(18,6)) AS amount_spent_inr, TRY_CAST(NULLIF(_airbyte_data:cpc::STRING, \'\') AS NUMBER(18,6)) AS cpc, TRY_CAST(NULLIF(_airbyte_data:cpm::STRING, \'\') AS NUMBER(18,6)) AS cpm, TRY_CAST(NULLIF(_airbyte_data:link_clicks::STRING, \'\') AS NUMBER(38,0)) AS link_clicks, _airbyte_data:_ab_source_file_url::STRING AS source_file, TRY_TO_TIMESTAMP_TZ(_airbyte_data:_ab_source_file_last_modified::STRING) AS source_file_modified_at FROM SNITCH_DB.MAPLEMONK._airbyte_raw_meta_product_spends_catalogue QUALIFY ROW_NUMBER() OVER ( PARTITION BY _airbyte_data:day::STRING, _airbyte_data:sku_with_size::STRING ORDER BY TRY_TO_TIMESTAMP_TZ(_airbyte_data:_ab_source_file_last_modified::STRING) DESC ) = 1;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            