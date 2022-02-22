{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE Maplemonk.public.Customer_Master AS select fi.customer_id,concat(c.first_name,\' \',c.last_name) name,c.email,c.phone,fi.source ,min(order_timestamp) Acquisition_Date ,fi.Acquisition_Product ,datediff(day,min(order_timestamp),current_date) days_since_first_purchase ,max(case when fi.order_timestamp=fi1.max_order_date then Product_name end) last_product ,sum(sales) total_spend ,count(distinct order_id) total_orders ,count(distinct item_id) total_items ,max(case when fi.order_timestamp=fi1.max_order_date then order_id end) last_order_id ,max(order_timestamp) last_purchase_date ,datediff(day,max(order_timestamp),current_date) days_since_last_purchase ,case when count(distinct order_id)=0 then null else datediff(day,min(order_timestamp),max(order_timestamp))/count(distinct order_id) end Avg_days_between_purchases ,sum(fi.discount) discount ,case when sum(fi.sales)=0 then null else sum(fi.discount)/sum(fi.sales) end discount_percent from maplemonk.public.FACT_ITEMS fi left join maplemonk.public.Shopifyindia_customers c on fi.customer_id = c.id left join (select customer_id,max(order_timestamp) max_order_date from maplemonk.public.FACT_ITEMS where source=\'Shopify\' group by customer_id)fi1 on fi.customer_id=fi1.customer_id where fi.customer_id is not null and fi.source=\'Shopify\' and order_timestamp is not null and source=\'Shopify\' group by fi.customer_id,concat(c.first_name,\' \',c.last_name),c.email,c.phone,fi.source,fi.Acquisition_Product;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MapleMonk.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        