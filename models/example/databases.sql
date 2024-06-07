{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE bsc_db.maplemonk.CUSTOMER_RETENTION_BY_PURCHASE_DATE AS WITH InitialResult AS ( SELECT distinct customer_id_final, product_category as category, sku as PRODUCT_NAME_MAPPED, discount_code, last_day(order_date) as order_month, order_date::date as order_date FROM bsc_db.maplemonk.bsc_db_sales_consolidated where brand = \'Bombay Shaving Company\' ), structuredInitialResult AS ( SELECT distinct customer_id_final, PRODUCT_NAME_MAPPED, category, discount_code, last_day(order_date) as order_month, order_date FROM InitialResult ), combineResult AS( select ori.*,date1,PRODUCT_NAME_MAPPED1,category1,order_date1,discount_code1 from structuredInitialResult ori left join (select customer_id_final,order_month as date1, PRODUCT_NAME_MAPPED as PRODUCT_NAME_MAPPED1, category as category1 , discount_code as discount_code1, order_date as order_date1 from structuredInitialResult)dup on ori.customer_id_final = dup.customer_id_final ) select c.*, cm.customer_number, cm.email, datediff(\'month\',order_month,date1) as next_month from combineResult c left join ( select customer_id_final as cid,customer_number,email from ( select distinct customer_id_final ,concat(+91,right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10)) as customer_number ,email ,row_number() over(partition by customer_id_final order by customer_number,email )rw from bsc_db.maplemonk.bsc_db_sales_consolidated where brand = \'Bombay Shaving Company\' ) where rw = 1 )as cm on cm.cid = c.customer_id_final where next_month >=0",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BSC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        