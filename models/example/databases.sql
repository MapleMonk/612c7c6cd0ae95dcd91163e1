{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.Customer_Master_Intermediate AS select fi.customer_id, concat(c.first_name,\' \',c.last_name) name, c.email, c.phone, fi.source, min(fi.order_timestamp::date) Acquisition_Date, fi.Acquisition_Product, datediff(day,min(order_timestamp),current_date) days_since_first_purchase, max(case when fi.order_timestamp=fi1.max_order_date then Product_name end) last_product, ifnull(sum(LINE_ITEM_SALES),0) total_spend_inr, count(distinct order_id) total_orders, case when total_orders = 1 then \'1 order\' when total_orders = 2 then \'2 orders\' when total_orders > 3 then \'>3 orders\' end as total_orders_buckets, count(distinct LINE_ITEM_ID) total_items, max(case when fi.order_timestamp=fi1.max_order_date then order_id end) last_order_id, datediff(day,max(order_timestamp),current_date) days_since_last_purchase, case when count(distinct order_id)=0 then null else datediff(day,min(order_timestamp),max(order_timestamp))/count(distinct order_id) end Avg_days_between_purchases, sum(fi.DISCOUNT) discount_inr, case when sum(fi.LINE_ITEM_SALES)=0 then null else sum(fi.DISCOUNT)/sum(fi.LINE_ITEM_SALES) end discount_percent from snitch_db.maplemonk.FACT_ITEMS_SNITCH fi left join (select * from (select *, count(id) over(partition by id order by _AIRBYTE_EMITTED_AT desc)rw from snitch_db.maplemonk.Shopify_All_customers)x where rw=1) c on fi.customer_id = c.id left join (select customer_id, max(order_timestamp) max_order_date from snitch_db.maplemonk.FACT_ITEMS_SNITCH where source=\'Shopify\' group by customer_id )fi1 on fi.customer_id=fi1.customer_id where fi.customer_id is not null and fi.source=\'Shopify\' and order_timestamp is not null group by fi.customer_id, concat(c.first_name,\' \',c.last_name), c.email, c.phone, fi.source, fi.Acquisition_Product; CREATE OR REPLACE TABLE snitch_db.maplemonk.Customer_Master_Snitch AS select x.*, rcm.category from(select c.*, m.Score as M_Score,r.Score as R_Score,f.Score as F_Score, (r.Score*1/3+f.Score*1/3+m.Score*1/3) as RFM_Score from (select *, row_number() over(order by TOTAL_SPEND_INR desc)/ (select count(distinct CUSTOMER_ID) from snitch_db.maplemonk.Customer_Master_Intermediate) as Percentile_M, row_number() over(order by DAYS_SINCE_LAST_PURCHASE asc,CUSTOMER_ID)/(select count(distinct CUSTOMER_ID) from snitch_db.maplemonk.Customer_Master_Intermediate) as Percentile_R, case when AVG_DAYS_BETWEEN_PURCHASES=0 then 0.4 else row_number() over(partition by case when AVG_DAYS_BETWEEN_PURCHASES=0 then 0 else 1 end order by AVG_DAYS_BETWEEN_PURCHASES asc,CUSTOMER_ID)/(select count(distinct CUSTOMER_ID) from snitch_db.maplemonk.Customer_Master_Intermediate) end as Percentile_F from snitch_db.maplemonk.Customer_Master_Intermediate) c left join snitch_db.maplemonk.RFM_Score m on c.Percentile_M > m.\"Lower Limit\" and c.Percentile_M<=m.\"Upper Limit\" left join snitch_db.maplemonk.RFM_Score r on c.Percentile_R > r.\"Lower Limit\" and c.Percentile_R<=r.\"Upper Limit\" left join snitch_db.maplemonk.RFM_Score f on c.Percentile_F > f.\"Lower Limit\" and c.Percentile_F<=f.\"Upper Limit\")x left join snitch_db.maplemonk.RFM_CUSTOMER_CATEGORY_MAPPING rcm on x.M_Score::int>rcm.M_START::int and x.M_Score::int<=M_END::int and x.R_Score::int>rcm.R_START::int and x.R_Score::int<=R_END::int and x.F_Score::int>rcm.F_START::int and x.F_Score::int<=F_END::int;",
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
                        