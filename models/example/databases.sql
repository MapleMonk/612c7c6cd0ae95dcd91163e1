{{ config(
            materialized='table',
                post_hook={
                    "sql": "DELETE FROM `kerala-ayurveda-wh.MapleMonk.shipmonk_fact_warehouses` WHERE source_airbyte_emitted_at_date >= DATE_SUB(CURRENT_DATE(\'Asia/Kolkata\'), INTERVAL 10 DAY); INSERT INTO `kerala-ayurveda-wh.MapleMonk.shipmonk_fact_warehouses` WITH staged_warehouses AS ( SELECT SAFE_CAST(JSON_VALUE(SAFE.PARSE_JSON(data), \'$.id\') AS INT64) AS id, JSON_VALUE(SAFE.PARSE_JSON(data), \'$.identifier\') AS identifier, JSON_VALUE(SAFE.PARSE_JSON(data), \'$.name\') AS name, JSON_VALUE(SAFE.PARSE_JSON(data), \'$.timezone\') AS timezone, JSON_VALUE(SAFE.PARSE_JSON(data), \'$.warehouse_wms_type\') AS warehouse_wms_type, SAFE_CAST(JSON_VALUE(SAFE.PARSE_JSON(data), \'$.enabled\') AS BOOL) AS enabled, STRUCT( JSON_VALUE(SAFE.PARSE_JSON(data), \'$.address.name\') AS name, JSON_VALUE(SAFE.PARSE_JSON(data), \'$.address.company\') AS company, JSON_VALUE(SAFE.PARSE_JSON(data), \'$.address.street1\') AS street1, JSON_VALUE(SAFE.PARSE_JSON(data), \'$.address.street2\') AS street2, JSON_VALUE(SAFE.PARSE_JSON(data), \'$.address.street3\') AS street3, JSON_VALUE(SAFE.PARSE_JSON(data), \'$.address.city\') AS city, JSON_VALUE(SAFE.PARSE_JSON(data), \'$.address.state\') AS state, JSON_VALUE(SAFE.PARSE_JSON(data), \'$.address.postal_code\') AS postal_code, JSON_VALUE(SAFE.PARSE_JSON(data), \'$.address.phone\') AS phone, SAFE_CAST(JSON_VALUE(SAFE.PARSE_JSON(data), \'$.address.residential\') AS BOOL) AS residential, SAFE_CAST(JSON_VALUE(SAFE.PARSE_JSON(data), \'$.address.verified\') AS BOOL) AS verified, JSON_VALUE(SAFE.PARSE_JSON(data), \'$.address.country\') AS country ) AS address, SAFE.PARSE_JSON(data) AS raw_data, _airbyte_ab_id AS source_airbyte_ab_id, _airbyte_emitted_at AS source_airbyte_emitted_at, DATE(_airbyte_emitted_at, \'Asia/Kolkata\') AS source_airbyte_emitted_at_date, _airbyte_normalized_at AS source_airbyte_normalized_at, _airbyte_shipmonk_get_list_of_warehouses_hashid AS source_airbyte_hashid, CURRENT_TIMESTAMP() AS bq_load_ts FROM `kerala-ayurveda-wh.MapleMonk.shipmonk_get_list_of_warehouses` WHERE DATE(_airbyte_emitted_at, \'Asia/Kolkata\') >= DATE_SUB(CURRENT_DATE(\'Asia/Kolkata\'), INTERVAL 10 DAY) ) SELECT * FROM staged_warehouses;",
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
            