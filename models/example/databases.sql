{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.next_purchase_intermediate as select concat(last_colour,\' \', last_category) last_purchase, concat(next_colour,\' \', next_category) next_purchase, number, list, share from( select *, row_number() over (partition by last_colour, last_category order by number desc) list, number/sum(number) over (partition by last_colour,last_category) Share from ( select b.colour last_colour, b.category last_category, a.colour next_colour, a.category next_category, count(*) number from ( (select row_number() over (partition by customer_id order by order_id) order_number,* from (select dense_rank() over (partition by order_id order by gross_sales,product_id desc) rw, * from snitch_db.maplemonk.fact_items_snitch) where rw = 1) )a left join ( select distinct customer_id,order_number, colour, category from (select row_number() over (partition by customer_id order by order_id) order_number,* from (select dense_rank() over (partition by order_id order by gross_sales,product_id desc) rw, * from snitch_db.maplemonk.fact_items_snitch) where rw = 1) ) b on a.order_number = (b.order_number + 1) and a.customer_id = b.customer_id where b.order_number is not null and b.customer_id is not null group by 1,2,3,4 order by 5 desc ) ) where list in (1,2,3) ; create or replace table snitch_db.maplemonk.next_purchase as select a.last_purchase, next_purchase1, next_purchase2, next_purchase3, share1, share2, share3 from (select last_purchase , next_purchase as next_purchase1 , share as share1 from snitch_db.maplemonk.next_purchase_intermediate where list = 1) a left join (select last_purchase , next_purchase as next_purchase2 , share as share2 from snitch_db.maplemonk.next_purchase_intermediate where list = 2) b on a.last_purchase = b.last_purchase left join (select last_purchase , next_purchase as next_purchase3 , share as share3 from snitch_db.maplemonk.next_purchase_intermediate where list = 3) c on a.last_purchase = c.last_purchase ; CREATE OR REPLACE TABLE snitch_db.maplemonk.Customer_Master_Intermediate AS select fi.customer_id, concat(c.first_name,\' \',c.last_name) name, em.email, ph.phone, fi.source, min(fi.order_timestamp::date) Acquisition_Date, fi.Acquisition_Product, datediff(day,min(order_timestamp),current_date) days_since_first_purchase, max(case when fi.order_timestamp=fi1.max_order_date then concat(colour,\' \', category) end) last_product, ifnull(sum(LINE_ITEM_SALES),0) total_spend_inr, count(distinct order_id) total_orders, case when total_orders = 1 then \'1 order\' when total_orders = 2 then \'2 orders\' when total_orders = 3 then \'3 orders\' when total_orders > 3 then \'>3 orders\' end as total_orders_buckets, count(distinct LINE_ITEM_ID) total_items, max(case when fi.order_timestamp=fi1.max_order_date then order_id end) last_order_id, datediff(day,max(order_timestamp),current_date) days_since_last_purchase, case when count(distinct order_id)=0 then null else datediff(day,min(order_timestamp),max(order_timestamp))/count(distinct order_id) end Avg_days_between_purchases, sum(fi.DISCOUNT) discount_inr, case when sum(fi.LINE_ITEM_SALES)=0 then null else sum(fi.DISCOUNT)/sum(fi.LINE_ITEM_SALES) end discount_percent from snitch_db.maplemonk.FACT_ITEMS_SNITCH fi left join (select * from (select *, count(id) over(partition by id order by _AIRBYTE_EMITTED_AT desc)rw from snitch_db.maplemonk.Shopify_All_customers)x where rw=1) c on fi.customer_id = c.id left join (select customer_id, max(order_timestamp) max_order_date from snitch_db.maplemonk.FACT_ITEMS_SNITCH where source=\'Shopify\' group by customer_id )fi1 on fi.customer_id=fi1.customer_id left join (select * from ( select customer_id, phone, row_number() over (partition by customer_id order by order_timestamp::date desc) rw from snitch_db.maplemonk.fact_items_snitch where phone is not null) where rw=1) ph on ph.customer_id = fi.customer_id left join (select * from ( select customer_id, email, row_number() over (partition by customer_id order by order_timestamp::date desc) rw from snitch_db.maplemonk.fact_items_snitch where email is not null) where rw=1) em on em.customer_id = fi.customer_id where fi.customer_id is not null and fi.source=\'Shopify\' and order_timestamp is not null group by fi.customer_id, concat(c.first_name,\' \',c.last_name), em.email, ph.phone, fi.source, fi.Acquisition_Product ; CREATE OR REPLACE TABLE snitch_db.maplemonk.Customer_Master_Snitch AS select x.*, rcm.category, np.next_purchase1, np.next_purchase2, np.next_purchase3 from(select c.*, m.Score as M_Score,r.Score as R_Score,f.Score as F_Score, (r.Score*1/3+f.Score*1/3+m.Score*1/3) as RFM_Score from (select *, row_number() over(order by TOTAL_SPEND_INR desc)/ (select count(distinct CUSTOMER_ID) from snitch_db.maplemonk.Customer_Master_Intermediate) as Percentile_M, row_number() over(order by DAYS_SINCE_LAST_PURCHASE asc,CUSTOMER_ID)/(select count(distinct CUSTOMER_ID) from snitch_db.maplemonk.Customer_Master_Intermediate) as Percentile_R, case when AVG_DAYS_BETWEEN_PURCHASES=0 then 0.4 else row_number() over(partition by case when AVG_DAYS_BETWEEN_PURCHASES=0 then 0 else 1 end order by AVG_DAYS_BETWEEN_PURCHASES asc,CUSTOMER_ID)/(select count(distinct CUSTOMER_ID) from snitch_db.maplemonk.Customer_Master_Intermediate) end as Percentile_F from snitch_db.maplemonk.Customer_Master_Intermediate) c left join snitch_db.maplemonk.RFM_Score m on c.Percentile_M > m.\"Lower Limit\" and c.Percentile_M<=m.\"Upper Limit\" left join snitch_db.maplemonk.RFM_Score r on c.Percentile_R > r.\"Lower Limit\" and c.Percentile_R<=r.\"Upper Limit\" left join snitch_db.maplemonk.RFM_Score f on c.Percentile_F > f.\"Lower Limit\" and c.Percentile_F<=f.\"Upper Limit\")x left join snitch_db.maplemonk.RFM_CUSTOMER_CATEGORY_MAPPING rcm on x.M_Score::int>rcm.M_START::int and x.M_Score::int<=M_END::int and x.R_Score::int>rcm.R_START::int and x.R_Score::int<=R_END::int and x.F_Score::int>rcm.F_START::int and x.F_Score::int<=F_END::int left join snitch_db.maplemonk.next_purchase np on np.last_purchase = x.last_product ;",
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
                        