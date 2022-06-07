{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table lilgoodness_db.maplemonk.customer_master_intermediate as select fi.customer_id_final, max(name) as Name, max(fi.email) email, fi.phone, min(Acquisition_Date) Acquisition_Date , min(case when fi.order_date = Acquisition_Date then marketplace end) as Marketplace, min(case when fi.order_date = Acquisition_Date then channel end) as Acquisition_Marketing_Channel, fi.acquisition_product, datediff(day,min(order_date),current_date) days_since_first_purchase, max(case when fi.order_date=fi1.max_order_date then PRODUCTNAME end) last_product, ifnull(sum(fi.selling_price),0) total_sales_inr, count(distinct fi.order_id) total_orders, count(distinct fi.line_item_id) total_items, max(case when fi.order_date=fi1.max_order_date then order_id end) last_order_id, datediff(day,max(fi.order_date),current_date) days_since_last_purchase, case when count(distinct order_id)=0 then null else datediff(day,min(order_date),max(order_date))/count(distinct order_id) end Avg_days_between_purchases, sum(fi.DISCOUNT) discount_inr, case when sum(fi.selling_price)=0 then null else sum(fi.DISCOUNT)/sum(fi.selling_price) end discount_percent from lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG fi left join (select customer_id_final, max(order_date) max_order_date from lilgoodness_db.maplemonk.SALES_CONSOLIDATED_LG group by customer_id_final )fi1 on fi.customer_id_final=fi1.customer_id_final where fi.customer_id_final is not null and order_date is not null group by fi.customer_id_final, fi.phone, fi.Acquisition_Product; create or replace table lilgoodness_db.maplemonk.customer_master_LG as select x.*, rcm.category from (select c.*, m.Score as M_Score,r.Score as R_Score,f.Score as F_Score, (r.Score*1/3+f.Score*1/3+m.Score*1/3) as RFM_Score from (select *, row_number () over (order by TOTAL_Sales_INR desc)/ (select count (distinct CUSTOMER_ID_FINAL) from lilgoodness_db.maplemonk.Customer_Master_Intermediate) as Percentile_M, row_number() over(order by DAYS_SINCE_LAST_PURCHASE asc,CUSTOMER_ID_FINAL)/(select count(distinct CUSTOMER_ID_FINAL) from lilgoodness_db.maplemonk.Customer_Master_Intermediate) as Percentile_R, case when AVG_DAYS_BETWEEN_PURCHASES=0 then 0.4 else row_number () over (partition by case when AVG_DAYS_BETWEEN_PURCHASES=0 then 0 else 1 end order by AVG_DAYS_BETWEEN_PURCHASES asc,CUSTOMER_ID_FINAL) /(select count(distinct CUSTOMER_ID_FINAL) from lilgoodness_db.maplemonk.Customer_Master_Intermediate) end as Percentile_F from lilgoodness_db.maplemonk.Customer_Master_Intermediate) c left join lilgoodness_db.maplemonk.RFM_Score m on c.Percentile_M > m.\"Lower Limit\" and c.Percentile_M<=m.\"Upper Limit\" left join lilgoodness_db.maplemonk.RFM_Score r on c.Percentile_R > r.\"Lower Limit\" and c.Percentile_R<=r.\"Upper Limit\" left join lilgoodness_db.maplemonk.RFM_Score f on c.Percentile_F > f.\"Lower Limit\" and c.Percentile_F<=f.\"Upper Limit\")x left join lilgoodness_db.maplemonk.RFM_CUSTOMER_CATEGORY_MAPPING rcm on x.M_Score::int>rcm.M_START::int and x.M_Score::int<=M_END::int and x.R_Score::int>rcm.R_START::int and x.R_Score::int<=R_END::int and x.F_Score::int>rcm.F_START::int and x.F_Score::int<=F_END::int;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from LILGOODNESS_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        