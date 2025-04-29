{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prd_db.beardo.dwh_vip_customers_data as select order_timestamp::Date order_Date, new_customer_flag, a.customer_id, ordeR_name, case when discount_code like \'%necbref%\' then \'NECBREF\' when discount_code like \'%necpage%\' then \'NECPAGE\' when discount_code like \'%nexiref%\' then \'NECIREF\' when discount_code like \'%necpri%\' then \'NECPRI\' end as discount_coupon_prefix, REGEXP_SUBSTR(a.tags, \'nector-coin-spent::([0-9]+)\', 1, 1, \'e\', 1) AS nector_coin_spent, case when lower(b.tags) like \'%vip%\' then \'VIP\' end as VIP_customer_flag, case when c.customer_id is not null then 1 else 0 end vip_customer_with_kits_on_first_order, sum(total_sales) total_sales from prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS a left join prd_db.beardo.DWH_SHOPIFY_ALL_CUSTOMERS b on a.customer_id = b.id left join ( select distinct customer_id from prd_db.beardo.dwh_SHOPIFY_FACT_ITEMS where new_customer_flag = \'New\' and product_name = \'Beardo VIP Club Kit\' ) c on a.customer_id = c.customer_id where vip_customer_flag = \'VIP\' group by 1,2,3,4,5,6,7,8 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from PRD_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            