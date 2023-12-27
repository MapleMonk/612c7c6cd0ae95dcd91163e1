{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table RPSG_db.maplemonk.customer_master_intermediate as select fi.customer_id_final, last_product_booked.phone, fi.acquisition_product, max(last_product_booked.customer_name) as Name, max(last_product_booked.email) email, min(fi.order_date) first_order_date, min(Acquisition_Date) Acquisition_Date , max(fi.order_date) Last_Booked_Order_Date, min(case when fi.order_date = Acquisition_Date then marketplace end) as Marketplace, min(case when fi.order_date = Acquisition_Date then channel end) as Acquisition_Marketing_Channel, max(case when fi.order_date=fi1.max_order_date then fi.order_id end) last_order_id, max(case when fi.order_date=fi1.max_order_date then fi.reference_code end) last_reference_code, max(last_product.order_Date) last_order_date, max(last_product.final_channel) last_order_Marketing_Channel, max(last_product.SKU) last_SKU, max(last_product.productname) last_product, max(last_product.product_name_mapped) last_product_name_mapped, max(last_product.category) last_product_category, max(last_product.report_category) last_product_report_category, max(last_product_booked.SKU) last_Booked_SKU, max(last_product_booked.productname) last_booked_product, max(last_product_booked.product_name_mapped) last_product_name_mapped, max(last_product_booked.category) last_product_booked_category, max(last_product_booked.report_category) last_product_booked_report_category, div0(sum(ifnull(case when fi.order_date=fi1.max_order_date then selling_price end,0)),ifnull(count(distinct case when fi.order_date=fi1.max_order_date then fi.order_id end),0)) last_order_value, sum(case when fi.order_date=fi1.max_order_date then discount else 0 end) last_order_discount, max(case when fi.order_date=fi1.max_order_date then fi.payment_mode end) last_order_payment_mode, count(distinct case when lower(payment_mode) like \'%cod%\' then fi.order_id end) COD_Orders, count(distinct case when lower(payment_mode) like any (\'%prepaid%\',\'%online%\') then fi.order_id end) Prepaid_Orders, sum(ifnull(fi.selling_price,0)) total_sales_inr, count(distinct fi.order_id) total_orders, count(distinct fi.line_item_id) total_items, sum(ifnull(fi.suborder_quantity,0)) total_quantity, count(distinct case when lower(fi.final_status) like any (\'%rto%\',\'%return%\') then fi.order_id end) RTO_ORDER_COUNT, count(distinct case when lower(fi.final_status) like any (\'%cancel%\') then fi.order_id end) CANCEL_ORDER_COUNT, count(distinct case when shop_name = \'Shopify_India\' then fi.order_id end ) as total_shopify_orders, count(distinct case when shop_name = \'CRED\' then fi.order_id end ) as total_CRED_orders, datediff(day,min(fi.order_date),current_date) days_since_first_purchase, datediff(month,min(fi.order_date),current_date) months_since_first_purchase, datediff(day,max(fi.order_date),current_date) days_since_last_booked_purchase, datediff(month,max(fi.order_date),current_date) months_since_last_booked_purchase, datediff(day,max(last_product.order_Date),current_date) days_since_last_purchase, datediff(month,max(last_product.order_Date),current_date) months_since_last_purchase, case when count(distinct fi.order_id)=0 then null else datediff(day,min(fi.order_date),max(fi.order_date))/count(distinct fi.order_id) end Avg_days_between_purchases, sum(fi.DISCOUNT) discount_inr, case when sum(fi.selling_price)=0 then null else sum(fi.DISCOUNT)/sum(fi.selling_price) end discount_percent from RPSG_db.maplemonk.SALES_CONSOLIDATED_DRV fi left join (select customer_id_final, max(order_date) max_order_date from RPSG_db.maplemonk.SALES_CONSOLIDATED_DRV group by customer_id_final ) fi1 on fi.customer_id_final=fi1.customer_id_final left join (select * from (select * ,row_number() over (partition by customer_id_final order by order_date desc) ranking from (select * from (select order_id, customer_id_final, email, phone, customer_name, order_date, final_channel, SKU, product_name_mapped, productname, report_category, category, row_number() over (partition by order_id order by selling_price desc) rw from RPSG_db.maplemonk.SALES_CONSOLIDATED_DRV where not(lower(final_status) like any (\'%cancel%\',\'%return%\',\'%rto%\')) ) where rw = 1 ) ) where ranking = 1 )last_product on fi.customer_id_final=last_product.customer_id_final left join (select * from (select * ,row_number() over (partition by customer_id_final order by order_date desc) ranking from (select * from (select order_id, customer_id_final, email, phone, customer_name, order_date, final_channel, SKU, product_name_mapped, productname, report_category, category, row_number() over (partition by order_id order by selling_price desc) rw from RPSG_db.maplemonk.SALES_CONSOLIDATED_DRV ) where rw = 1 ) ) where ranking = 1 )last_product_booked on fi.customer_id_final=last_product_booked.customer_id_final where fi.customer_id_final is not null and fi.order_date is not null group by fi.customer_id_final, last_product_booked.phone, fi.Acquisition_Product ; create or replace table RPSG_db.maplemonk.customer_master_DRV as select x.*, rcm.category from ( select c.* , m.Score as M_Score ,r.Score as R_Score ,f.Score as F_Score ,(r.Score*1/3+f.Score*1/3+m.Score*1/3) as RFM_Score from (select * ,row_number () over (order by TOTAL_Sales_INR desc)/ (select count (distinct CUSTOMER_ID_FINAL) from RPSG_db.maplemonk.Customer_Master_Intermediate) as Percentile_M ,row_number() over(order by DAYS_SINCE_LAST_PURCHASE asc,CUSTOMER_ID_FINAL)/(select count(distinct CUSTOMER_ID_FINAL) from RPSG_db.maplemonk.Customer_Master_Intermediate) as Percentile_R ,case when AVG_DAYS_BETWEEN_PURCHASES=0 then 0.4 else row_number () over (partition by case when AVG_DAYS_BETWEEN_PURCHASES=0 then 0 else 1 end order by AVG_DAYS_BETWEEN_PURCHASES asc,CUSTOMER_ID_FINAL) /(select count(distinct CUSTOMER_ID_FINAL) from RPSG_db.maplemonk.Customer_Master_Intermediate) end as Percentile_F from RPSG_db.maplemonk.Customer_Master_Intermediate ) c left join RPSG_db.maplemonk.RFM_Score m on c.Percentile_M > m.\"Lower Limit\" and c.Percentile_M<=m.\"Upper Limit\" left join RPSG_db.maplemonk.RFM_Score r on c.Percentile_R > r.\"Lower Limit\" and c.Percentile_R<=r.\"Upper Limit\" left join RPSG_db.maplemonk.RFM_Score f on c.Percentile_F > f.\"Lower Limit\" and c.Percentile_F<=f.\"Upper Limit\" )x left join RPSG_db.maplemonk.RFM_CUSTOMER_CATEGORY_MAPPING rcm on x.M_Score::int>rcm.M_START::int and x.M_Score::int<=M_END::int and x.R_Score::int>rcm.R_START::int and x.R_Score::int<=R_END::int and x.F_Score::int>rcm.F_START::int and x.F_Score::int<=F_END::int; Create or replace table rpsg_db.maplemonk.customers_master_by_phone_number_intermediate as select customer_name ,email ,phone ,city ,pin_code ,product_name ,last_ordeR_date ,first_order_date ,case when DATEDIFF(day,last_ordeR_date , getdate()::DATE) > 365 then \'Dormant\' else \'Not Dormant\' end as dormant_flag ,case when DATEDIFF(day,last_ordeR_date , getdate()::DATE) < 365 and DATEDIFF(day,last_ordeR_date , getdate()::DATE) > 180 then \'Winback\' else \'Not Winback\' end as Winback_flag ,case when DATEDIFF(day,last_ordeR_date , getdate()::DATE) < 365 and DATEDIFF(day,last_ordeR_date , getdate()::DATE) > 90 then \'Stable\' else \'Not Stable\' end as Stable_flag ,case when date_trunc(\'month\',first_ordeR_date) = date_trunc(\'month\',getdate()::date) then \'New customer in current month\' else \'Old Customer\' end as new_customer_current_month_flag from (select customer_name ,email ,case when len(replace(a.phone, \' \',\'\')) = 10 then concat(\'+91\',replace(a.phone, \' \',\'\')) else replace(a.phone, \' \',\'\') end as phone ,city ,pin_code ,product_name_mapped product_name ,a.ordeR_Date::Date last_order_Date ,b.first_ordeR_date::date first_ordeR_date ,row_number() over (partition by order_id order by selling_price desc) rw from (select * from RPSG_db.maplemonk.SALES_CONSOLIDATED_DRV where lower(marketplace) like any (\'%shopify%\', \'%woocommerce%\') )a left join ( select case when len(replace(phone, \' \',\'\')) = 10 then concat(\'+91\',replace(phone, \' \',\'\')) else replace(phone, \' \',\'\') end as phone ,max(ordeR_Date) last_ordeR_Date ,min(order_date) first_ordeR_date from RPSG_db.maplemonk.SALES_CONSOLIDATED_DRV where lower(marketplace) like any (\'%shopify%\', \'%woocommerce%\') group by 1 ) b on case when len(replace(a.phone, \' \',\'\')) = 10 then concat(\'+91\',replace(a.phone, \' \',\'\')) else replace(a.phone, \' \',\'\') end = b.phone and a.order_date = last_ordeR_Date where b.phone is not null and b.last_ordeR_date is not null )where rw = 1 ; create or replace table rpsg_db.maplemonk.customers_master_by_phone_number as select * from rpsg_db.maplemonk.customers_master_by_phone_number_intermediate union all select m.customer_name, m.email, m.phone, n.city, m.pin_code, m.product_name, m.last_order_date, m.first_order_Date ,case when DATEDIFF(day,last_ordeR_date , getdate()::DATE) > 365 then \'Dormant\' else \'Not Dormant\' end as dormant_flag ,case when DATEDIFF(day,last_ordeR_date , getdate()::DATE) < 365 and DATEDIFF(day,last_ordeR_date , getdate()::DATE) > 180 then \'Winback\' else \'Not Winback\' end as Winback_flag ,case when DATEDIFF(day,last_ordeR_date , getdate()::DATE) < 365 and DATEDIFF(day,last_ordeR_date , getdate()::DATE) > 90 then \'Stable\' else \'Not Stable\' end as Stable_flag ,case when date_trunc(\'month\',first_ordeR_date) = date_trunc(\'month\',getdate()::date) then \'New customer in current month\' else \'Old Customer\' end as new_customer_current_month_flag from ( select a.customer_name, a.email, case when len(replace(a.phone, \' \',\'\')) = 10 then concat(\'+91\',replace(a.phone, \' \',\'\')) else replace(a.phone, \' \',\'\') end phone, left(a.ordeR_date,10) last_order_Date, b.first_ordeR_date::date first_ordeR_date, a.pin_code, a.item_name product_name, row_number() over (partition by a.phone order by item_name) rw from rpsg_db.maplemonk.drv1920dump a left join ( select case when len(replace(phone, \' \',\'\')) = 10 then concat(\'+91\',replace(phone, \' \',\'\')) else replace(phone, \' \',\'\') end phone ,max(left(order_date,10)) last_order_date ,min(left(order_date,10)) first_order_date from rpsg_db.maplemonk.drv1920dump group by 1 ) b on case when len(replace(a.phone, \' \',\'\')) = 10 then concat(\'+91\',replace(a.phone, \' \',\'\')) else replace(a.phone, \' \',\'\') end = b.phone and left(a.order_date,10) = last_ordeR_Date where b.phone is not null and b.last_ordeR_date is not null ) m left join (select * from ( select pin_code, city, row_number() over (partition by pin_code order by city) rw from RPSG_db.maplemonk.SALES_CONSOLIDATED_DRV )where rw = 1) n on m.pin_code = n.pin_code where m.rw = 1 and m.phone not in (select distinct phone from rpsg_db.maplemonk.customers_master_by_phone_number_intermediate);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from RPSG_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        