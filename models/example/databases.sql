{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK_DEMO_DB.MAPLEMONK.MAPLEMONK_DEMO_DB_CUSTOMER_PRODUCT_RETENTION_BY_PURCHASE_MONTH AS WITH InitialResult AS ( SELECT distinct customer_id_final, product_category as category, product_name_final as PRODUCT_NAME_MAPPED, last_day(order_date) as order_month, order_date as order_date FROM MAPLEMONK_DEMO_DB.MAPLEMONK.MAPLEMONK_DEMO_DB_sales_consolidated where lower(marketplace) like any(\'%shopify%\',\'%woocommerce%\',\'%website%\') ), structuredInitialResult AS ( SELECT distinct customer_id_final, PRODUCT_NAME_MAPPED, category, last_day(order_date) as order_month, order_date FROM InitialResult ), combineResult AS( select ori.*,date1,PRODUCT_NAME_MAPPED1,category1,order_date1 from structuredInitialResult ori left join (select customer_id_final,order_month as date1, PRODUCT_NAME_MAPPED as PRODUCT_NAME_MAPPED1, category as category1 , order_date as order_date1 from structuredInitialResult)dup on ori.customer_id_final = dup.customer_id_final ) select c.*, cm.phone, cm.email, datediff(\'month\',order_month,date1) as next_month from combineResult c left join ( select * from ( select customer_id_final as cid ,phone ,email ,row_number() over(partition by customer_id_final order by 1)rw from MAPLEMONK_DEMO_DB.MAPLEMONK.MAPLEMONK_DEMO_DB_sales_consolidated where lower(marketplace) like any(\'%shopify%\',\'%woocommerce%\',\'%website%\') )where rw = 1 ) as cm on cm.cid = c.customer_id_final where next_month >=0",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MAPLEMONK_DEMO_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            