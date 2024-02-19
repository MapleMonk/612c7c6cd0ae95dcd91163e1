{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SELECT_DB.MAPLEMONK.SELECT_DB_ABC_LATEST_ORDER AS WITH CTE AS ( select ORDER_DATE, REFERENCE_CODE, REPLACE(PHONE, \'+91\', \'\') AS MOBILE_NO, DELIVERY_STATUS, SUM(QUANTITY) as total_units, ROW_NUMBER() OVER(PARTITION BY MOBILE_NO ORDER BY ORDER_DATE DESC) AS rn from SELECT_DB.MAPLEMONK.SELECT_DB_ORDER_FULFILLMENT_REPORT where FINAL_MARKETPLACE = \'SHOPIFY_KYARI_CO\' group by REFERENCE_CODE, ORDER_DATE, PHONE, DELIVERY_STATUS ) select * from cte where rn = 1;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SELECT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        