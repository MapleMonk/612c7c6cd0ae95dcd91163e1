{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prd_db.beardo.dwh_overall_repeat as select \'3-month\' as repeat_period ,a.date date ,count(distinct case when rw = 1 then customer_id_final end) all_customers ,count(distinct case when rw > 1 then customer_id_final end) repeat_customers from (select distinct date_trunc(\'month\',ordeR_timestamp::Date) date from prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS order by 1)a left join (select order_date, customer_id_final, row_number() over (partition by customer_id_final order by order_date asc)rw from ( select distinct order_Date, customer_id_final from (select * from prd_db.beardo.dwh_sales_consolidated where shop_name =\'SHOPIFY_BEARDO\' and lower(final_Status) not in (\'cancelled\',\'returned\'))))b on datediff(month, a.date, date_trunc(\'month\',b.order_date::date) ) in (0,1,2) group by 1,2 union all select \'6-month\' as repeat_period ,a.date date ,count(distinct case when rw = 1 then customer_id_final end) all_customers ,count(distinct case when rw > 1 then customer_id_final end) repeat_customers from (select distinct date_trunc(\'month\',ordeR_timestamp::Date) date from prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS order by 1)a left join (select order_date, customer_id_final, row_number() over (partition by customer_id_final order by order_date asc )rw from ( select distinct order_Date, customer_id_final from (select * from prd_db.beardo.dwh_sales_consolidated where shop_name =\'SHOPIFY_BEARDO\' and lower(final_Status) not in (\'cancelled\',\'returned\'))))b on datediff(month, a.date, date_trunc(\'month\',b.order_date::date) ) in (0,1,2,3,4,5) group by 1,2 union all select \'12-month\' as repeat_period ,a.date date ,count(distinct case when rw = 1 then customer_id_final end) all_customers ,count(distinct case when rw > 1 then customer_id_final end) repeat_customers from (select distinct date_trunc(\'month\',ordeR_timestamp::Date) date from prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS order by 1)a left join (select order_date, customer_id_final, row_number() over (partition by customer_id_final order by order_date asc )rw from ( select distinct order_Date, customer_id_final from (select * from prd_db.beardo.dwh_sales_consolidated where shop_name =\'SHOPIFY_BEARDO\' and lower(final_Status) not in (\'cancelled\',\'returned\'))))b on datediff(month, a.date, date_trunc(\'month\',b.order_date::date) ) in (0,1,2,3,4,5,6,7,8,9,10,11) group by 1,2 ; create or replace table prd_db.beardo.dwh_product_Category_repeat as select \'3-month\' as repeat_period ,date date ,product_category ,count(distinct case when rw = 1 then customer_id_final end) all_customers ,count(distinct case when rw > 1 then customer_id_final end) repeat_customers from ( select a.date ,a.product_category ,customer_id_final ,row_number() over (partition by a.date, a.product_category, customer_id_final order by order_date asc) rw from (select distinct date_trunc(\'month\',ordeR_date::Date) date, product_category from prd_db.beardo.dwh_sales_consolidated where order_date::Date > \'2023-03-01\' order by 1)a left join ( select distinct order_Date, customer_id_final, product_category from (select * from prd_db.beardo.dwh_sales_consolidated where shop_name =\'SHOPIFY_BEARDO\' and lower(final_Status) not in (\'cancelled\',\'returned\')))b on datediff(month, a.date, date_trunc(\'month\',b.order_date::date) ) in (0,1,2) and lower(a.product_category) = lower(b.product_Category)) group by 1,2,3 union all select \'6-month\' as repeat_period ,date date ,product_category ,count(distinct case when rw = 1 then customer_id_final end) all_customers ,count(distinct case when rw > 1 then customer_id_final end) repeat_customers from ( select a.date ,a.product_category ,customer_id_final ,row_number() over (partition by a.date, a.product_category, customer_id_final order by order_date asc) rw from (select distinct date_trunc(\'month\',ordeR_date::Date) date, product_category from prd_db.beardo.dwh_sales_consolidated where order_date::Date > \'2023-03-01\' order by 1)a left join ( select distinct order_Date, customer_id_final, product_category from (select * from prd_db.beardo.dwh_sales_consolidated where shop_name =\'SHOPIFY_BEARDO\' and lower(final_Status) not in (\'cancelled\',\'returned\')))b on datediff(month, a.date, date_trunc(\'month\',b.order_date::date) ) in (0,1,2,3,4,5) and lower(a.product_category) = lower(b.product_Category)) group by 1,2,3 union all select \'12-month\' as repeat_period ,date date ,product_category ,count(distinct case when rw = 1 then customer_id_final end) all_customers ,count(distinct case when rw > 1 then customer_id_final end) repeat_customers from ( select a.date ,a.product_category ,customer_id_final ,row_number() over (partition by a.date, a.product_category, customer_id_final order by order_date asc) rw from (select distinct date_trunc(\'month\',ordeR_date::Date) date, product_category from prd_db.beardo.dwh_sales_consolidated where order_date::Date > \'2023-03-01\' order by 1)a left join ( select distinct order_Date, customer_id_final, product_category from (select * from prd_db.beardo.dwh_sales_consolidated where shop_name =\'SHOPIFY_BEARDO\' and lower(final_Status) not in (\'cancelled\',\'returned\')))b on datediff(month, a.date, date_trunc(\'month\',b.order_date::date) ) in (0,1,2,3,4,5,6,7,8,9,10,11) and lower(a.product_category) = lower(b.product_Category)) group by 1,2,3 ; create or replace table prd_db.beardo.dwh_child_product_repeat as select \'3-month\' as repeat_period ,date ,CHILD_PRODUCT_NAME ,count(distinct case when rw = 1 then customer_id_final end) all_customers ,count(distinct case when rw > 1 then customer_id_final end) repeat_customers from ( select a.date ,a.CHILD_PRODUCT_NAME ,customer_id_final ,row_number() over (partition by a.date, a.CHILD_PRODUCT_NAME, customer_id_final order by order_date asc) rw from (select distinct date_trunc(\'month\',ordeR_date::Date) date, CHILD_PRODUCT_NAME from prd_db.beardo.dwh_sales_consolidated where order_date::Date > \'2023-03-01\' order by 1)a left join ( select distinct order_Date, customer_id_final, CHILD_PRODUCT_NAME from (select * from prd_db.beardo.dwh_sales_consolidated where shop_name =\'SHOPIFY_BEARDO\' and lower(final_Status) not in (\'cancelled\',\'returned\')))b on datediff(month, a.date, date_trunc(\'month\',b.order_date::date) ) in (0,1,2) and lower(a.CHILD_PRODUCT_NAME) = lower(b.CHILD_PRODUCT_NAME)) group by 1,2,3 union all select \'6-month\' as repeat_period ,date ,CHILD_PRODUCT_NAME ,count(distinct case when rw = 1 then customer_id_final end) all_customers ,count(distinct case when rw > 1 then customer_id_final end) repeat_customers from ( select a.date ,a.CHILD_PRODUCT_NAME ,customer_id_final ,row_number() over (partition by a.date, a.CHILD_PRODUCT_NAME, customer_id_final order by order_date asc) rw from (select distinct date_trunc(\'month\',ordeR_date::Date) date, CHILD_PRODUCT_NAME from prd_db.beardo.dwh_sales_consolidated where order_date::Date > \'2023-03-01\' order by 1)a left join ( select distinct order_Date, customer_id_final, CHILD_PRODUCT_NAME from (select * from prd_db.beardo.dwh_sales_consolidated where shop_name =\'SHOPIFY_BEARDO\' and lower(final_Status) not in (\'cancelled\',\'returned\')))b on datediff(month, a.date, date_trunc(\'month\',b.order_date::date) ) in (0,1,2,3,4,5) and lower(a.CHILD_PRODUCT_NAME) = lower(b.CHILD_PRODUCT_NAME)) group by 1,2,3 union all select \'12-month\' as repeat_period ,date ,CHILD_PRODUCT_NAME ,count(distinct case when rw = 1 then customer_id_final end) all_customers ,count(distinct case when rw > 1 then customer_id_final end) repeat_customers from ( select a.date ,a.CHILD_PRODUCT_NAME ,customer_id_final ,row_number() over (partition by a.date, a.CHILD_PRODUCT_NAME, customer_id_final order by order_date asc) rw from (select distinct date_trunc(\'month\',ordeR_date::Date) date, CHILD_PRODUCT_NAME from prd_db.beardo.dwh_sales_consolidated where order_date::Date > \'2023-03-01\' order by 1)a left join ( select distinct order_Date, customer_id_final, CHILD_PRODUCT_NAME from (select * from prd_db.beardo.dwh_sales_consolidated where shop_name =\'SHOPIFY_BEARDO\' and lower(final_Status) not in (\'cancelled\',\'returned\')))b on datediff(month, a.date, date_trunc(\'month\',b.order_date::date) ) in (0,1,2,3,4,5,6,7,8,9,10,11) and lower(a.CHILD_PRODUCT_NAME) = lower(b.CHILD_PRODUCT_NAME)) group by 1,2,3 ; create or replace table prd_db.beardo.dwh_product_sub_category_repeat as select \'3-month\' as repeat_period ,date ,product_sub_category ,count(distinct case when rw = 1 then customer_id_final end) all_customers ,count(distinct case when rw > 1 then customer_id_final end) repeat_customers from ( select a.date ,a.product_sub_category ,customer_id_final ,row_number() over (partition by a.date, a.product_sub_category, customer_id_final order by order_date asc) rw from (select distinct date_trunc(\'month\',ordeR_date::Date) date, product_sub_category from prd_db.beardo.dwh_sales_consolidated where order_date::Date > \'2023-03-01\' order by 1)a left join ( select distinct order_Date, customer_id_final, product_sub_category from (select * from prd_db.beardo.dwh_sales_consolidated where shop_name =\'SHOPIFY_BEARDO\' and lower(final_Status) not in (\'cancelled\',\'returned\')))b on datediff(month, a.date, date_trunc(\'month\',b.order_date::date) ) in (0,1,2) and lower(a.product_sub_category) = lower(b.product_sub_category)) group by 1,2,3 union all select \'6-month\' as repeat_period ,date ,product_sub_category ,count(distinct case when rw = 1 then customer_id_final end) all_customers ,count(distinct case when rw > 1 then customer_id_final end) repeat_customers from ( select a.date ,a.product_sub_category ,customer_id_final ,row_number() over (partition by a.date, a.product_sub_category, customer_id_final order by order_date asc) rw from (select distinct date_trunc(\'month\',ordeR_date::Date) date, product_sub_category from prd_db.beardo.dwh_sales_consolidated where order_date::Date > \'2023-03-01\' order by 1)a left join ( select distinct order_Date, customer_id_final, product_sub_category from (select * from prd_db.beardo.dwh_sales_consolidated where shop_name =\'SHOPIFY_BEARDO\' and lower(final_Status) not in (\'cancelled\',\'returned\')))b on datediff(month, a.date, date_trunc(\'month\',b.order_date::date) ) in (0,1,2,3,4,5) and lower(a.product_sub_category) = lower(b.product_sub_category)) group by 1,2,3 union all select \'12-month\' as repeat_period ,date ,product_sub_category ,count(distinct case when rw = 1 then customer_id_final end) all_customers ,count(distinct case when rw > 1 then customer_id_final end) repeat_customers from ( select a.date ,a.product_sub_category ,customer_id_final ,row_number() over (partition by a.date, a.product_sub_category, customer_id_final order by order_date asc) rw from (select distinct date_trunc(\'month\',ordeR_date::Date) date, product_sub_category from prd_db.beardo.dwh_sales_consolidated where order_date::Date > \'2023-03-01\' order by 1)a left join ( select distinct order_Date, customer_id_final, product_sub_category from (select * from prd_db.beardo.dwh_sales_consolidated where shop_name =\'SHOPIFY_BEARDO\' and lower(final_Status) not in (\'cancelled\',\'returned\')))b on datediff(month, a.date, date_trunc(\'month\',b.order_date::date) ) in (0,1,2,3,4,5,6,7,8,9,10,11) and lower(a.product_sub_category) = lower(b.product_sub_category)) group by 1,2,3 ;",
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
            