{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE prd_db.beardo.dwh_CUSTOMER_RETENTION_BY_PURCHASE_DATE AS WITH InitialResult AS ( SELECT distinct customer_id_final, product_category as category, product_name_final as PRODUCT_NAME_MAPPED, last_day(order_date) as order_month, order_date::date as order_date from prd_db.beardo.dwh_sales_consolidated where lower(shop_name) like any (\'%shopify%\',\'%woocommer%\') ), Start_Month as ( select * from ( Select *,rank() over(partition by customer_id_final order by order_date asc) rw from InitialResult )where rw = 1 ), combineResult AS( select ori.*,date1,PRODUCT_NAME_MAPPED1,category1,order_date1 from Start_Month ori left join (select customer_id_final,order_month as date1, PRODUCT_NAME_MAPPED as PRODUCT_NAME_MAPPED1, category as category1 , order_date as order_date1 from InitialResult)dup on ori.customer_id_final = dup.customer_id_final ), month_level_customers as ( select order_month,count(distinct customer_id_final) total_Customers_count from combineResult group by 1 ) select c.*, cm.customer_number, cm.email, cm.STATE, cm.acquisition_product, datediff(\'month\',c.order_month,date1) as next_month, mlc.total_Customers_count from combineResult c left join ( select customer_id_final as cid,customer_number,email,STATE,acquisition_product from ( select distinct customer_id_final ,concat(+91,right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10)) as customer_number ,email ,STATE ,acquisition_product ,row_number() over(partition by customer_id_final order by customer_number,email )rw from prd_db.beardo.dwh_sales_consolidated ) where rw = 1 )as cm on cm.cid = c.customer_id_final left join month_level_customers mlc on c.order_month = mlc.order_month where next_month >=0 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from DATALAKE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            