{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE BBK_DB.MAPLEMONK.CUSTOMER_RETENTION_BBK AS SELECT year(Acquisition_date) Acquisition_Year, monthname(Acquisition_date) Acquisition_Month, last_day(Acquisition_date) Year_Month, acquisition_product, acquisition_outlet, acquisition_source, acquisition_ordertype, ordersource, count(distinct customer_id_final) total_customers, count(distinct case when last_day(Acquisition_Date)=last_day(order_date) then order_id end) total_orders, sum(case when last_day(Acquisition_Date)=last_day(order_date) then Selling_price_inr end) total_sales, count(distinct case when last_day(Acquisition_Date)<>last_day(order_date) then customer_id_final end) mn_c, count(distinct case when last_day(Acquisition_Date)<>last_day(order_date) then order_id end) mn_o, sum(case when last_day(Acquisition_Date)<>last_day(order_date) then Selling_price_inr end) mn_s, count(distinct case when acquisition_product=product_name and last_day(Acquisition_Date)<>last_day(order_date) then customer_id_final end) product_mn_c, count(distinct case when acquisition_product=product_name and last_day(Acquisition_Date)<>last_day(order_date) then order_id end) product_mn_o, sum(case when acquisition_product=product_name and last_day(Acquisition_Date)<>last_day(order_date) then Selling_price_inr end) product_mn_s, count(distinct case when last_day(dateadd(month,1,Acquisition_Date))=last_day(order_date) then customer_id_final end) m2_c, count(distinct case when last_day(dateadd(month,2,Acquisition_Date))=last_day(order_date) then customer_id_final end) m3_c, count(distinct case when last_day(dateadd(month,3,Acquisition_Date))=last_day(order_date) then customer_id_final end) m4_c, count(distinct case when last_day(dateadd(month,4,Acquisition_Date))=last_day(order_date) then customer_id_final end) m5_c, count(distinct case when last_day(dateadd(month,5,Acquisition_Date))=last_day(order_date) then customer_id_final end) m6_c, count(distinct case when last_day(dateadd(month,6,Acquisition_Date))=last_day(order_date) then customer_id_final end) m7_c, count(distinct case when last_day(dateadd(month,7,Acquisition_Date))=last_day(order_date) then customer_id_final end) m8_c, count(distinct case when last_day(dateadd(month,8,Acquisition_Date))=last_day(order_date) then customer_id_final end) m9_c, count(distinct case when last_day(dateadd(month,9,Acquisition_Date))=last_day(order_date) then customer_id_final end) m10_c, count(distinct case when last_day(dateadd(month,10,Acquisition_Date))=last_day(order_date) then customer_id_final end) m11_c, count(distinct case when last_day(dateadd(month,11,Acquisition_Date))=last_day(order_date) then customer_id_final end) m12_c, count(distinct case when last_day(dateadd(month,1,Acquisition_Date))=last_day(order_date) then order_id end) m2_o, count(distinct case when last_day(dateadd(month,2,Acquisition_Date))=last_day(order_date) then order_id end) m3_o, count(distinct case when last_day(dateadd(month,3,Acquisition_Date))=last_day(order_date) then order_id end) m4_o, count(distinct case when last_day(dateadd(month,4,Acquisition_Date))=last_day(order_date) then order_id end) m5_o, count(distinct case when last_day(dateadd(month,5,Acquisition_Date))=last_day(order_date) then order_id end) m6_o, count(distinct case when last_day(dateadd(month,6,Acquisition_Date))=last_day(order_date) then order_id end) m7_o, count(distinct case when last_day(dateadd(month,7,Acquisition_Date))=last_day(order_date) then order_id end) m8_o, count(distinct case when last_day(dateadd(month,8,Acquisition_Date))=last_day(order_date) then order_id end) m9_o, count(distinct case when last_day(dateadd(month,9,Acquisition_Date))=last_day(order_date) then order_id end) m10_o, count(distinct case when last_day(dateadd(month,10,Acquisition_Date))=last_day(order_date) then order_id end) m11_o, count(distinct case when last_day(dateadd(month,11,Acquisition_Date))=last_day(order_date) then order_id end) m12_o, sum(case when last_day(dateadd(month,1,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m2_s, sum(case when last_day(dateadd(month,2,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m3_s, sum(case when last_day(dateadd(month,3,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m4_s, sum(case when last_day(dateadd(month,4,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m5_s, sum(case when last_day(dateadd(month,5,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m6_s, sum(case when last_day(dateadd(month,6,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m7_s, sum(case when last_day(dateadd(month,7,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m8_s, sum(case when last_day(dateadd(month,8,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m9_s, sum(case when last_day(dateadd(month,9,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m10_s, sum(case when last_day(dateadd(month,10,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m11_s, sum(case when last_day(dateadd(month,11,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m12_s, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,1,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m2_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,2,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m3_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,3,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m4_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,4,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m5_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,5,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m6_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,6,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m7_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,7,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m8_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,8,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m9_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,9,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m10_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,10,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m11_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,11,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m12_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,1,Acquisition_Date))= last_day(order_date) then order_id end) product_m2_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,2,Acquisition_Date))= last_day(order_date) then order_id end) product_m3_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,3,Acquisition_Date))= last_day(order_date) then order_id end) product_m4_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,4,Acquisition_Date))= last_day(order_date) then order_id end) product_m5_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,5,Acquisition_Date))= last_day(order_date) then order_id end) product_m6_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,6,Acquisition_Date))= last_day(order_date) then order_id end) product_m7_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,7,Acquisition_Date))= last_day(order_date) then order_id end) product_m8_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,8,Acquisition_Date))= last_day(order_date) then order_id end) product_m9_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,9,Acquisition_Date))= last_day(order_date) then order_id end) product_m10_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,10,Acquisition_Date))= last_day(order_date) then order_id end) product_m11_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,11,Acquisition_Date))= last_day(order_date) then order_id end) product_m12_o, sum(case when acquisition_product=product_name and last_day(dateadd(month,1,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m2_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,2,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m3_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,3,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m4_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,4,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m5_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,5,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m6_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,6,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m7_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,7,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m8_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,8,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m9_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,9,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m10_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,10,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m11_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,11,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m12_s from ( select customer_id_final, order_id, acquisition_product, acquisition_outlet, acquisition_source, acquisition_ordertype, itemname as product_name, ordersource, outletname as outlet, ordertype, saledate as order_Date, item_price as Selling_price_inr, acquisition_date from BBK_DB.maplemonk.fact_items_bbk where customer_id_final is not null and lower(orderstatus) <> \'cancelled\' )res group by year(Acquisition_Date), monthname(Acquisition_Date), last_day(Acquisition_Date), acquisition_source, acquisition_outlet, acquisition_ordertype, Acquisition_Product, ordersource union all SELECT year(Acquisition_Date) Acquisition_Year, monthname(Acquisition_Date) Acquisition_Month, last_day(Acquisition_Date) Year_Month, acquisition_product, acquisition_outlet, acquisition_source, acquisition_ordertype, \'All\' as ordersource, count(distinct customer_id_final) total_customers, count(distinct case when last_day(Acquisition_Date)=last_day(order_date) then order_id end) total_orders, sum(case when last_day(Acquisition_Date)=last_day(order_date) then Selling_price_inr end) total_sales, count(distinct case when last_day(Acquisition_Date)<>last_day(order_date) then customer_id_final end) mn_c, count(distinct case when last_day(Acquisition_Date)<>last_day(order_date) then order_id end) mn_o, sum(case when last_day(Acquisition_Date)<>last_day(order_date) then Selling_price_inr end) mn_s, count(distinct case when acquisition_product=product_name and last_day(Acquisition_Date)<>last_day(order_date) then customer_id_final end) product_mn_c, count(distinct case when acquisition_product=product_name and last_day(Acquisition_Date)<>last_day(order_date) then order_id end) product_mn_o, sum(case when acquisition_product=product_name and last_day(Acquisition_Date)<>last_day(order_date) then Selling_price_inr end) product_mn_s, count(distinct case when last_day(dateadd(month,1,Acquisition_Date))=last_day(order_date) then customer_id_final end) m2_c, count(distinct case when last_day(dateadd(month,2,Acquisition_Date))=last_day(order_date) then customer_id_final end) m3_c, count(distinct case when last_day(dateadd(month,3,Acquisition_Date))=last_day(order_date) then customer_id_final end) m4_c, count(distinct case when last_day(dateadd(month,4,Acquisition_Date))=last_day(order_date) then customer_id_final end) m5_c, count(distinct case when last_day(dateadd(month,5,Acquisition_Date))=last_day(order_date) then customer_id_final end) m6_c, count(distinct case when last_day(dateadd(month,6,Acquisition_Date))=last_day(order_date) then customer_id_final end) m7_c, count(distinct case when last_day(dateadd(month,7,Acquisition_Date))=last_day(order_date) then customer_id_final end) m8_c, count(distinct case when last_day(dateadd(month,8,Acquisition_Date))=last_day(order_date) then customer_id_final end) m9_c, count(distinct case when last_day(dateadd(month,9,Acquisition_Date))=last_day(order_date) then customer_id_final end) m10_c, count(distinct case when last_day(dateadd(month,10,Acquisition_Date))=last_day(order_date) then customer_id_final end) m11_c, count(distinct case when last_day(dateadd(month,11,Acquisition_Date))=last_day(order_date) then customer_id_final end) m12_c, count(distinct case when last_day(dateadd(month,1,Acquisition_Date))=last_day(order_date) then order_id end) m2_o, count(distinct case when last_day(dateadd(month,2,Acquisition_Date))=last_day(order_date) then order_id end) m3_o, count(distinct case when last_day(dateadd(month,3,Acquisition_Date))=last_day(order_date) then order_id end) m4_o, count(distinct case when last_day(dateadd(month,4,Acquisition_Date))=last_day(order_date) then order_id end) m5_o, count(distinct case when last_day(dateadd(month,5,Acquisition_Date))=last_day(order_date) then order_id end) m6_o, count(distinct case when last_day(dateadd(month,6,Acquisition_Date))=last_day(order_date) then order_id end) m7_o, count(distinct case when last_day(dateadd(month,7,Acquisition_Date))=last_day(order_date) then order_id end) m8_o, count(distinct case when last_day(dateadd(month,8,Acquisition_Date))=last_day(order_date) then order_id end) m9_o, count(distinct case when last_day(dateadd(month,9,Acquisition_Date))=last_day(order_date) then order_id end) m10_o, count(distinct case when last_day(dateadd(month,10,Acquisition_Date))=last_day(order_date) then order_id end) m11_o, count(distinct case when last_day(dateadd(month,11,Acquisition_Date))=last_day(order_date) then order_id end) m12_o, sum(case when last_day(dateadd(month,1,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m2_s, sum(case when last_day(dateadd(month,2,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m3_s, sum(case when last_day(dateadd(month,3,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m4_s, sum(case when last_day(dateadd(month,4,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m5_s, sum(case when last_day(dateadd(month,5,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m6_s, sum(case when last_day(dateadd(month,6,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m7_s, sum(case when last_day(dateadd(month,7,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m8_s, sum(case when last_day(dateadd(month,8,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m9_s, sum(case when last_day(dateadd(month,9,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m10_s, sum(case when last_day(dateadd(month,10,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m11_s, sum(case when last_day(dateadd(month,11,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) m12_s, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,1,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m2_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,2,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m3_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,3,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m4_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,4,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m5_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,5,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m6_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,6,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m7_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,7,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m8_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,8,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m9_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,9,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m10_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,10,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m11_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,11,Acquisition_Date))= last_day(order_date) then customer_id_final end) product_m12_c, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,1,Acquisition_Date))= last_day(order_date) then order_id end) product_m2_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,2,Acquisition_Date))= last_day(order_date) then order_id end) product_m3_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,3,Acquisition_Date))= last_day(order_date) then order_id end) product_m4_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,4,Acquisition_Date))= last_day(order_date) then order_id end) product_m5_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,5,Acquisition_Date))= last_day(order_date) then order_id end) product_m6_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,6,Acquisition_Date))= last_day(order_date) then order_id end) product_m7_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,7,Acquisition_Date))= last_day(order_date) then order_id end) product_m8_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,8,Acquisition_Date))= last_day(order_date) then order_id end) product_m9_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,9,Acquisition_Date))= last_day(order_date) then order_id end) product_m10_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,10,Acquisition_Date))= last_day(order_date) then order_id end) product_m11_o, count(distinct case when acquisition_product=product_name and last_day(dateadd(month,11,Acquisition_Date))= last_day(order_date) then order_id end) product_m12_o, sum(case when acquisition_product=product_name and last_day(dateadd(month,1,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m2_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,2,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m3_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,3,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m4_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,4,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m5_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,5,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m6_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,6,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m7_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,7,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m8_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,8,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m9_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,9,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m10_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,10,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m11_s, sum(case when acquisition_product=product_name and last_day(dateadd(month,11,Acquisition_Date))=last_day(order_date) then Selling_price_inr end) product_m12_s from ( select customer_id_final, order_id, acquisition_product, acquisition_source, acquisition_outlet, acquisition_ordertype, ordersource, outletname as outlet, ordertype, itemname as product_name, saledate as order_Date, item_price as Selling_price_inr, acquisition_date from BBK_DB.maplemonk.fact_items_bbk where customer_id_final is not null and lower(orderstatus) <> \'cancelled\' )res group by year(Acquisition_Date), monthname(Acquisition_Date), last_day(Acquisition_Date), acquisition_source, Acquisition_Product, acquisition_outlet, acquisition_ordertype; create or replace table BBK_DB.maplemonk.customer_master_intermediate as select fi.customer_id_final, max(fi.CUSTOMERNAME) as Name, max(fi.email) email, fi.guest_phone, min(Acquisition_Date) Acquisition_Date , min(case when fi.saledate = Acquisition_Date then ordersource end) as Acquisition_ordersource, fi.acquisition_product, datediff(day,min(saledate),current_date) days_since_first_purchase, max(case when fi.saledate=fi1.max_order_date then itemname end) last_product, ifnull(sum(fi.item_price),0) total_sales_inr, count(distinct fi.order_id) total_orders, count(distinct fi.order_item_id) total_items, max(case when fi.saledate=fi1.max_order_date then order_id end) last_order_id, datediff(day,max(fi.saledate),current_date) days_since_last_purchase, case when count(distinct order_id)=0 then null else datediff(day,min(saledate),max(saledate))/count(distinct order_id) end Avg_days_between_purchases, sum(fi.order_discount) discount_inr, case when sum(fi.item_price)=0 then null else sum(fi.order_discount)/sum(fi.item_price) end discount_percent from BBK_DB.maplemonk.fact_items_bbk fi left join (select customer_id_final, max(saledate) max_order_date from BBK_DB.maplemonk.fact_items_bbk group by customer_id_final )fi1 on fi.customer_id_final=fi1.customer_id_final where fi.customer_id_final is not null and saledate is not null group by fi.customer_id_final, fi.guest_phone, fi.Acquisition_Product; create or replace table BBK_DB.maplemonk.customer_master_BBKK as select x.*, rcm.category from (select c.*, m.Score as M_Score,r.Score as R_Score,f.Score as F_Score, (r.Score*1/3+f.Score*1/3+m.Score*1/3) as RFM_Score from (select *, row_number () over (order by TOTAL_Sales_INR desc)/ (select count (distinct customer_id_final) from BBK_DB.maplemonk.Customer_Master_Intermediate) as Percentile_M, row_number() over(order by DAYS_SINCE_LAST_PURCHASE asc,customer_id_final)/(select count(distinct customer_id_final) from BBK_DB.maplemonk.Customer_Master_Intermediate) as Percentile_R, case when AVG_DAYS_BETWEEN_PURCHASES=0 then 0.4 else row_number () over (partition by case when AVG_DAYS_BETWEEN_PURCHASES=0 then 0 else 1 end order by AVG_DAYS_BETWEEN_PURCHASES asc,customer_id_final) /(select count(distinct customer_id_final) from BBK_DB.maplemonk.Customer_Master_Intermediate) end as Percentile_F from BBK_DB.maplemonk.Customer_Master_Intermediate) c left join BBK_DB.maplemonk.BBK_RFM_Score m on c.Percentile_M > m.\"Lower Limit\" and c.Percentile_M<=m.\"Upper Limit\" left join BBK_DB.maplemonk.BBK_RFM_Score r on c.Percentile_R > r.\"Lower Limit\" and c.Percentile_R<=r.\"Upper Limit\" left join BBK_DB.maplemonk.BBK_RFM_Score f on c.Percentile_F > f.\"Lower Limit\" and c.Percentile_F<=f.\"Upper Limit\")x left join BBK_DB.maplemonk.BBK_RFM_CUSTOMER_CATEGORY_MAPPING rcm on x.M_Score::int>rcm.M_START::int and x.M_Score::int<=M_END::int and x.R_Score::int>rcm.R_START::int and x.R_Score::int<=R_END::int and x.F_Score::int>rcm.F_START::int and x.F_Score::int<=F_END::int ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BBK_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        