{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.gs_master_data_clean AS SELECT \"ASM\", TRY_TO_NUMBER(REGEXP_REPLACE(\"INF\", \'[^0-9.-]\', \'\')) AS \"INF\", \"URL\", \"STATE\", \"PERIOD\", \"REGION\", TRY_TO_NUMBER(REGEXP_REPLACE(\"SHORTAGE\", \'[^0-9.-]\', \'\')) AS \"SHORTAGE\", \"Audit Team\" AS AUDIT_TEAM, TRY_TO_NUMBER(REGEXP_REPLACE(\"Shortage %\", \'[^0-9.-]\', \'\')) AS \"Shortage %\", \"Store Name\" AS STORE_NAME, \"Store Type\" AS STORE_TYPE, \"Audit Month\" AS AUDIT_MONTH, \"Audit dates\" AS AUDIT_DATES, TRY_TO_NUMBER(REGEXP_REPLACE(\"Physical Qty\", \'[^0-9.-]\', \'\')) AS PHYSICAL_QTY, TRY_TO_NUMBER(REGEXP_REPLACE(\"Shortage Value\", \'[^0-9.-]\', \'\')) AS SHORTAGE_VALUE, TRY_TO_NUMBER(REGEXP_REPLACE(\"Shortage Value %\", \'[^0-9.-]\', \'\')) AS SHORTAGE_VALUE_PCT, TRY_TO_NUMBER(REGEXP_REPLACE(\"Total Value in MRP\", \'[^0-9.-]\', \'\')) AS TOTAL_VALUE_IN_MRP, TRY_TO_NUMBER(REGEXP_REPLACE(\"Physical Value in MRP\", \'[^0-9.-]\', \'\')) AS PHYSICAL_VALUE_IN_MRP, TRY_TO_NUMBER(REGEXP_REPLACE(\"Total Units (Excluding Non Trading Products\", \'[^0-9.-]\', \'\')) AS TOTAL_UNITS_EXCLUDING_NON_TRADING, \"_AIRBYTE_AB_ID\", \"_AIRBYTE_EMITTED_AT\", \"_AIRBYTE_NORMALIZED_AT\", \"_AIRBYTE_GS_MASTER_DATA_HASHID\" FROM snitch_db.maplemonk.gs_master_data;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            