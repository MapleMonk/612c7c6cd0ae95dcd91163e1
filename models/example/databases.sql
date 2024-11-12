{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table Maplemonk.CC_BR_Consolidated_GET_SALES_AND_TRAFFIC_REPORT_ASIN AS SELECT _airbyte_unique_key, sku, childAsin, parentAsin, dataEndTime, salesByAsin, dataStartTime, trafficByAsin, \'BE\' AS Country FROM maplemonk.CC_BR_BE_GET_SALES_AND_TRAFFIC_REPORT_ASIN UNION ALL SELECT _airbyte_unique_key, sku, childAsin, parentAsin, dataEndTime, salesByAsin, dataStartTime, trafficByAsin, \'GB\' AS Country FROM maplemonk.CC_BR_UK_GET_SALES_AND_TRAFFIC_REPORT_ASIN UNION ALL SELECT _airbyte_unique_key, sku, childAsin, parentAsin, dataEndTime, salesByAsin, dataStartTime, trafficByAsin, \'DE\' AS Country FROM maplemonk.CC_BR_DE_GET_SALES_AND_TRAFFIC_REPORT_ASIN UNION ALL SELECT _airbyte_unique_key, sku, childAsin, parentAsin, dataEndTime, salesByAsin, dataStartTime, trafficByAsin, \'ES\' AS Country FROM maplemonk.CC_BR_ES_GET_SALES_AND_TRAFFIC_REPORT_ASIN UNION ALL SELECT _airbyte_unique_key, sku, childAsin, parentAsin, dataEndTime, salesByAsin, dataStartTime, trafficByAsin, \'FR\' AS Country FROM maplemonk.CC_BR_FR_GET_SALES_AND_TRAFFIC_REPORT_ASIN UNION ALL SELECT _airbyte_unique_key, sku, childAsin, parentAsin, dataEndTime, salesByAsin, dataStartTime, trafficByAsin, \'IT\' AS Country FROM maplemonk.CC_BR_IT_GET_SALES_AND_TRAFFIC_REPORT_ASIN UNION ALL SELECT _airbyte_unique_key, sku, childAsin, parentAsin, dataEndTime, salesByAsin, dataStartTime, trafficByAsin, \'NL\' AS Country FROM maplemonk.CC_BR_NL_GET_SALES_AND_TRAFFIC_REPORT_ASIN UNION ALL SELECT _airbyte_unique_key, sku, childAsin, parentAsin, dataEndTime, salesByAsin, dataStartTime, trafficByAsin, \'SE\' AS Country FROM maplemonk.CC_BR_SE_GET_SALES_AND_TRAFFIC_REPORT_ASIN;",
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
            