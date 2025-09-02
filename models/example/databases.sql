{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.UF_MATHS AS WITH uf AS ( SELECT order_name,added_on,order_timestamp,marketplace_mapped,sku,shippingpackagecode,order_id,item_status,warehouse_name,tat,uc_created, saleorderitemcode,count(saleorderitemcode) as q1 FROM snitch_db.maplemonk.uf_count_check WHERE uc_created::DATE >= \'2025-04-01\' GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12 ), sf_raw AS ( SELECT CASE WHEN \"Shipping Package Code\" like \'%EMIZA%\' THEN \'SAPL_EMIZA\' WHEN \"Shipping Package Code\" like \'%SAPL2%\' THEN \'SAPL-WH2\' WHEN \"Shipping Package Code\" like \'%SAPLWH1%\' THEN \'SAPL-WH1\' WHEN \"Shipping Package Code\" like \'%OMNI%\' THEN \'OMNI\' ELSE NULL END AS facility, * FROM snitch_db.maplemonk.forward_timestamps WHERE created_timestamp >= \'2025-04-01\' ), sf AS ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY CONCAT(\"Display Order Code\",facility) ORDER BY created_timestamp ASC ) AS rn FROM sf_raw ) WHERE rn = 1 ), final1 as ( SELECT CASE WHEN TIMESTAMPDIFF(hour,order_timestamp,uc_created) > 5 THEN \'EXCHANGE\' ELSE \'OTHER\' END AS ORDER_CAT, CASE WHEN created_timestamp is null or TIMESTAMPDIFF(MIN,uc_created,created_timestamp) > 30 then \'DIRECT_UF\' ELSE \'NF\' END AS UF_TYPE ,* FROM uf LEFT JOIN sf ON CONCAT(uf.order_name,uf.warehouse_name) = CONCAT(sf.\"Display Order Code\",sf.facility) ), uc as ( select saleorderitemcode, sla_status from snitch_db.maplemonk.warehouse_sla_performance ), new as ( select f.*, u.sla_status as final_status from final1 f left join uc u on f.saleorderitemcode = u.saleorderitemcode ) select * from new ;",
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
            