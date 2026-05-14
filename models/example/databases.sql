{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.SFUCCHECK AS SELECT sf.*, uc.order_name as uc_statusas FROM snitch_db.maplemonk.fact_items_snitch sf LEFT JOIN snitch_db.maplemonk.warehouse_sla_performance uc ON sf.order_name = uc.order_name WHERE LOWER(COALESCE(tags, \'\')) NOT LIKE \'%eco%\' AND LOWER(COALESCE(tags, \'\')) NOT LIKE \'%rms%\' AND LOWER(COALESCE(tags, \'\')) NOT LIKE \'%exch%\' AND sf.order_timestamp >= \'2026-01-01\' and city_mapped != \'TEST\' AND uc.order_name is null and sf.order_status not in (\'CANCELLED\') order by order_timestamp asc ; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.UCLOGICCHECK AS WITH logic AS ( SELECT * FROM ( SELECT _airbyte_data:\"Order_No\"::STRING AS order_no, _airbyte_data:\"Remarks2\"::STRING AS remarks2, _airbyte_data:\"Gr_Number\"::STRING AS gr_number, _airbyte_data:\"New_Bill_No\"::STRING AS new_bill_no, _airbyte_data:\"Vouch_Code\"::STRING AS vouch_code, TO_DATE( _airbyte_data:\"Bill_Date\"::STRING, \'DD/MM/YYYY\' ) AS bill_date, ROW_NUMBER() OVER ( PARTITION BY _airbyte_data:\"Vouch_Code\"::STRING, _airbyte_data:\"New_Bill_No\"::STRING ORDER BY TO_DATE(_airbyte_data:\"Bill_Date\"::STRING, \'DD/MM/YYYY\') DESC ) AS rn FROM snitch_db.maplemonk._airbyte_raw_logicerpnew_get_sale_invoice ) a WHERE bill_date >= \'2026-05-10\' AND rn = 1 ), uc_base AS ( SELECT * FROM snitch_db.maplemonk.unicommerce_fact_items_intermediate_2024 WHERE marketplace_mapped NOT LIKE \'%STORE%\' AND invoice_date::DATE >= \'2026-05-10\' AND sku NOT IN (\'DUMMY-BOX-01\') ), non_flip AS ( SELECT uc.awb, uc.order_name, uc.dispatched_timestamp, uc.marketplace_mapped, uc.sku, le.order_no, uc.warehouse_name, uc.invoice_date FROM uc_base uc LEFT JOIN logic le ON uc.invoice_code = le.order_no WHERE uc.marketplace_mapped NOT IN ( \'NOON\',\'SAPL_WH_TRANSFERS\',\'FLIPKART_SOR\',\'MYNTRA_SJIT\', \'M-NOW\',\'SLIKK\',\'M_NOW_WESTBENGAL\',\'KNOT\', \'B2B CORPORATE CHANNEL\',\'ZILO\',\'INSTAMART\',\'SNITCH HQ\', \'CUSTOM\',\'FLIPKART\')), flip AS ( SELECT uc.awb, uc.order_name, uc.dispatched_timestamp, uc.marketplace_mapped, uc.sku, le.order_no, uc.warehouse_name, uc.invoice_date FROM uc_base uc LEFT JOIN logic le ON uc.awb = le.gr_number WHERE uc.marketplace_mapped = \'FLIPKART\' ) SELECT * FROM non_flip UNION ALL SELECT * FROM flip;",
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
            