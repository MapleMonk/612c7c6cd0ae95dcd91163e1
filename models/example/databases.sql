{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE RPSG_DB.maplemonk.CUSTOMER_RETENTION_BY_PRCHASE_MONTH_DELIVERED AS WITH InitialResult AS ( SELECT distinct customer_id_final, category, PRODUCT_NAME_MAPPED, last_day(order_date) as order_month, order_date as order_date FROM RPSG_DB.maplemonk.SALES_CONSOLIDATED_DRV where lower(marketplace) like any (\'%shopify%\',\'%woocommerce%\') AND UPPER(FINAL_STATUS) = \'DELIVERED\' ), structuredInitialResult AS ( SELECT distinct customer_id_final, PRODUCT_NAME_MAPPED, category, last_day(order_date) as order_month, order_date FROM InitialResult ), combineResult AS( select ori.*,date1,PRODUCT_NAME_MAPPED1,category1,order_date1 from structuredInitialResult ori left join (select customer_id_final,order_month as date1, PRODUCT_NAME_MAPPED as PRODUCT_NAME_MAPPED1, category as category1 , order_date as order_date1 from structuredInitialResult)dup on ori.customer_id_final = dup.customer_id_final ) select c.*, cm.phone, cm.email, datediff(\'month\',order_month,date1) as next_month from combineResult c left join (select customer_id_final as cid,phone,email from rpsg_db.maplemonk.customer_master_drv) as cm on cm.cid = c.customer_id_final where next_month >=0;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from RPSG_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        