{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table ttk_db.maplemonk.affiliate_fact_items as select traffic.date, sessions, addtocarts, checkouts, orders, round(revenue,2) revenue, cancelled_orders, round(cancelled_revenue,2) cancelled_revenue, round(realized_revenue,2) realized_revenue, round(earnings,2) earnings from ( select to_date(date, \'yyyymmdd\') date, sum(sessions) sessions, sum(addtocarts) addtocarts, sum(checkouts) checkouts from ttk_db.Maplemonk.GA4_GA4_Lovedepot_SESSIONS_USERS_BY_DATE where lower(sessionsourcemedium) like \'%icw %\' group by 1 order by 1 ) traffic left join (select order_timestamp::Date ordeR_Date, count(distinct ordeR_id) orders, sum(total_sales) revenue, count(distinct case when ordeR_Status = \'CANCELLED\' then ordeR_id end) cancelled_orders, sum(case when ordeR_Status = \'CANCELLED\' then total_Sales end) cancelled_Revenue, ifnull(revenue,0) - ifnull(cancelled_revenue,0) as realized_revenue, sum(earnings) earnings, from ( select ordeR_timestamp,order_id, ordeR_status, sum(total_sales) total_sales, case when order_Status <> \'CANCELLED\' and sum(total_sales) > 4000 then 0.15*sum(total_Sales) when ordeR_status <> \'CANCELLED\' and sum(total_sales) < 4000 then 0.2*sum(total_sales) end as earnings from ttk_db.Maplemonk.ttk_db_SHOPIFY_FACT_ITEMS where shop_name = \'Shopify_love_depot_india\' and lower(discount_codes) like \'%icw10%\' group by 1,2,3 ) group by 1 ) orders on traffic.date = orders.ordeR_Date; create or replace table ttk_db.maplemonk.affiliate_summary as select ordeR_timestamp, order_id, ordeR_name, sku, product_name, ordeR_status, final_utm_source, final_utm_channel, round(total_sales,2) total_sales from ttk_db.Maplemonk.ttk_db_SHOPIFY_FACT_ITEMS where shop_name = \'Shopify_love_depot_india\' and lower(discount_codes) like \'%icw10%\'",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from ttk_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            