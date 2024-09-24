{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE GLADFUL_DB.maplemonk.GLADFUL_DB_CUSTOMER_RETENTION_BY_PURCHASE_DATE AS WITH InitialResult AS ( SELECT distinct customer_id_final, CHILD_PRODUCT_CATEGORY as category, SKU_CODE_CHILD as SKU, CHILD_PRODUCT_NAME as PRODUCT_NAME_MAPPED, last_day(order_date) as order_month, order_date::date as order_date, FROM GLADFUL_DB.maplemonk.GLADFUL_sales_consolidated ), structuredInitialResult AS ( SELECT distinct customer_id_final, sku, PRODUCT_NAME_MAPPED, category, last_day(order_date) as order_month, order_date, FROM InitialResult ), combineResult AS( select ori.*,date1,PRODUCT_NAME_MAPPED1,category1,order_date1,sku1 from structuredInitialResult ori left join (select customer_id_final,order_month as date1, PRODUCT_NAME_MAPPED as PRODUCT_NAME_MAPPED1, category as category1 , sku as sku1, order_date as order_date1 from structuredInitialResult)dup on ori.customer_id_final = dup.customer_id_final ) select c.*, cm.customer_number, cm.email, datediff(\'month\',order_month,date1) as next_month from combineResult c left join ( select customer_id_final as cid,customer_number,email from ( select distinct customer_id_final ,concat(+91,right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10)) as customer_number ,email ,row_number() over(partition by customer_id_final order by customer_number,email )rw from GLADFUL_DB.maplemonk.GLADFUL_sales_consolidated ) where rw = 1 )as cm on cm.cid = c.customer_id_final where next_month >=0 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from GLADFUL_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            