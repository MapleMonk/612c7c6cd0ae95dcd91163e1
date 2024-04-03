{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.CUSTOMER_RETENTION_BY_PURCHASE_DATE_UPDATED AS WITH InitialResult AS ( SELECT customer_id_final, last_day(order_date) as order_month, sum(selling_price) as sales FROM snitch_db.snitch.ORDERS_fact where SOURCE_CHANNEL = \'SHOPIFY\' group by 1,2 ), combineResult AS( select ori.*,date1 from InitialResult ori left join (select customer_id_final, order_month as date1 from InitialResult)dup on ori.customer_id_final = dup.customer_id_final ) select c.*, cm.phone, cm.email, datediff(\'month\',order_month,date1) as next_month, 0.65 as gross_margin from combineResult c left join (select customer_id_final as cid,phone,email from snitch_db.snitch.customer_dim) as cm on cm.cid = c.customer_id_final where next_month >=0",
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
                        