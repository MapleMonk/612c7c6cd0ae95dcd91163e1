{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE or replace table SAI_ABHIJITH348_DB.Maplemonk.RFM_SCORE ( \"Lower Limit\" FLOAT,\"Upper Limit\" FLOAT,Score FLOAT); Insert into SAI_ABHIJITH348_DB.Maplemonk.RFM_SCORE values (0,0.1,10), (0.1,0.2,9), (0.2,0.3,8),(0.3,0.4,7),(0.4,0.5,6),(0.5,0.6,5),(0.6,0.7,4),(0.7,0.8,3),(0.8,0.9,2),(0.9,1,1); CREATE or replace TABLE SAI_ABHIJITH348_DB.Maplemonk.RFM_CUSTOMER_CATEGORY_MAPPING ( R_Start FLOAT,R_End FLOAT,F_Start FLOAT,F_End FLOAT,M_Start FLOAT,M_End FLOAT, Category VARCHAR(16777216)); Insert into SAI_ABHIJITH348_DB.Maplemonk.rfm_customer_category_mapping values (8,10,8,10,6,10,\'Champion\') ,(8,10,8,10,4,6,\'Frequent Customer with medium spend\') ,(8,10,8,10,0,4,\'Frequent Customer with low spend\') ,(6,8,8,10,0,10,\'Loyal Customers\') ,(8,10,6,8,0,10,\'Loyal Customers\') ,(4,6,0,6,0,10,\'Potential to become inactive\') ,(0,4,4,6,0,10,\'Potential to become inactive\') ,(6,8,6,8,0,10,\'Potential to become loyal Customers\') ,(8,10,4,6,0,10,\'Needs more engagement\') ,(6,8,0,6,0,10,\'Needs more engagement\') ,(8,10,0,4,0,10,\'New Customer/Promising Customers\') ,(0,6,6,10,0,10,\'Initiate re-engagement\') ,(0,4,0,4,0,10,\'Inactive Customers\'); create or replace table SAI_ABHIJITH348_DB.Maplemonk.SAI_ABHIJITH348_DB_next_purchase_intermediate as select last_product, next_product, number, list, share from( select *, row_number() over (partition by last_product order by number desc) list, number/sum(number) over (partition by last_product) Share from ( select b.product_name last_product, a.product_name next_product, count(*) number from ( select row_number() over (partition by customer_id_final order by order_id) order_number,* from (select dense_rank() over (partition by order_id order by selling_price,product_id desc) rw, * from SAI_ABHIJITH348_DB.Maplemonk.SAI_ABHIJITH348_DB_sales_consolidated where lower(shop_name) like any (\'%shopify%\',\'%woocommerce%\') ) where rw = 1 )a left join ( select distinct customer_id_final , order_number, product_name from (select row_number() over (partition by customer_id_final order by order_id) order_number,* from (select dense_rank() over (partition by order_id order by selling_price,product_id desc) rw, * from SAI_ABHIJITH348_DB.Maplemonk.SAI_ABHIJITH348_DB_sales_consolidated where lower(shop_name) like any (\'%shopify%\',\'%woocommerce%\') ) where rw = 1 ) ) b on a.order_number = (b.order_number + 1) and a.customer_id_final = b.customer_id_final where b.order_number is not null and b.customer_id_final is not null group by b.product_name, a.product_name order by 3 desc ) ) where list in (1,2,3); create or replace table SAI_ABHIJITH348_DB.Maplemonk.SAI_ABHIJITH348_DB_next_purchase as select a.last_product, next_product1, next_product2, next_product3, share1, share2, share3 from (select last_product , next_product as next_product1 , share as share1 from SAI_ABHIJITH348_DB.Maplemonk.SAI_ABHIJITH348_DB_next_purchase_intermediate where list = 1 ) a left join (select last_product ,next_product as next_product2 ,share as share2 from SAI_ABHIJITH348_DB.Maplemonk.SAI_ABHIJITH348_DB_next_purchase_intermediate where list = 2 ) b on a.last_product = b.last_product left join (select last_product ,next_product as next_product3 ,share as share3 from SAI_ABHIJITH348_DB.Maplemonk.SAI_ABHIJITH348_DB_next_purchase_intermediate where list = 3 ) c on a.last_product = c.last_product ; create or replace table SAI_ABHIJITH348_DB.Maplemonk.customer_master_intermediate as select fi.customer_id_final as MM_Customer_ID, max(fi.name) as Name, max(fi.email) Email, fi.phone as Phone, min(Acquisition_Date) Acquisition_Date , min(case when fi.order_date = Acquisition_Date then shop_name end) as Acquistion_Marketplace, min(case when fi.order_date = Acquisition_Date then source end) as Acquisition_Marketing_Channel, fi.acquisition_product as Acquisition_Product, datediff(day,min(order_date),current_date) Days_Since_First_Purchase, max(case when fi.order_date=fi1.max_order_date then PRODUCT_NAME end) Last_Product, ifnull(sum(fi.selling_price),0) Total_Sales_INR, count(distinct fi.order_id) Total_Orders, count(distinct case when lower(order_status) in (\'cancelled\',\'returned\',\'rto\') then fi.order_id end) Cancelled_Orders, count(distinct case when lower(order_status) in (\'returned\',\'rto\') then fi.order_id end) Returned_Orders, count(distinct case when lower(shop_name) like \'%shopify%\' then fi.order_id end ) as Total_Shopify_Orders, count(distinct case when lower(shop_name) like \'%cred%\' then fi.order_id end ) as Total_CRED_Orders, count(distinct fi.saleorderitemcode) Total_Items, max(case when fi.order_date=fi1.max_order_date then order_id end) Last_Order_Id, datediff(day,max(fi.order_date),current_date) Days_Since_Last_Purchase, case when count(distinct order_id)=0 then null else datediff(day,min(order_date),max(order_date))/count(distinct order_id) end Avg_days_between_purchases, sum(fi.DISCOUNT) discount_inr, case when sum(fi.selling_price)=0 then null else sum(fi.DISCOUNT)/sum(fi.selling_price) end discount_percent, sum(ifnull(days_in_shipment, 0)) AS days_in_shipment from SAI_ABHIJITH348_DB.Maplemonk.SAI_ABHIJITH348_DB_SALES_CONSOLIDATED fi left join (select customer_id_final, max(order_date) Max_Order_Date from SAI_ABHIJITH348_DB.Maplemonk.SAI_ABHIJITH348_DB_SALES_CONSOLIDATED group by customer_id_final )fi1 on fi.Customer_Id_Final=fi1.Customer_Id_Final where fi.Customer_Id_Final is not null and Order_Date is not null group by fi.Customer_Id_Final, fi.Phone, fi.Acquisition_Product; create or replace table SAI_ABHIJITH348_DB.Maplemonk.SAI_ABHIJITH348_DB_CUSTOMER_MASTER as select x.*, np.next_product1, np.next_product2, np.next_product3, np.share1, np.share2, np.share3, rcm.Category as Customer_Segment from (select c.*, m.Score as M_Score, r.Score as R_Score, f.Score as F_Score, (r.Score*1/3+f.Score*1/3+m.Score*1/3) as RFM_Score from (select *, row_number () over (order by TOTAL_Sales_INR desc)/ (select count (distinct MM_CUSTOMER_ID) from SAI_ABHIJITH348_DB.Maplemonk.Customer_Master_Intermediate) as Percentile_M, row_number() over(order by DAYS_SINCE_LAST_PURCHASE asc,MM_CUSTOMER_ID)/(select count(distinct MM_CUSTOMER_ID) from SAI_ABHIJITH348_DB.Maplemonk.Customer_Master_Intermediate) as Percentile_R, case when AVG_DAYS_BETWEEN_PURCHASES=0 then 0.4 else row_number () over (partition by case when AVG_DAYS_BETWEEN_PURCHASES=0 then 0 else 1 end order by AVG_DAYS_BETWEEN_PURCHASES asc,MM_CUSTOMER_ID) /(select count(distinct MM_CUSTOMER_ID) from SAI_ABHIJITH348_DB.Maplemonk.Customer_Master_Intermediate) end as Percentile_F from SAI_ABHIJITH348_DB.Maplemonk.Customer_Master_Intermediate) c left join SAI_ABHIJITH348_DB.Maplemonk.RFM_Score m on c.Percentile_M > m.\"Lower Limit\" and c.Percentile_M<=m.\"Upper Limit\" left join SAI_ABHIJITH348_DB.Maplemonk.RFM_Score r on c.Percentile_R > r.\"Lower Limit\" and c.Percentile_R<=r.\"Upper Limit\" left join SAI_ABHIJITH348_DB.Maplemonk.RFM_Score f on c.Percentile_F > f.\"Lower Limit\" and c.Percentile_F<=f.\"Upper Limit\" )x left join SAI_ABHIJITH348_DB.Maplemonk.RFM_CUSTOMER_CATEGORY_MAPPING rcm on x.M_Score::int>rcm.\"M_START\"::int and x.M_Score::int<=\"M_END\"::int and x.R_Score::int>rcm.\"R_START\"::int and x.R_Score::int<=\"R_END\"::int and x.F_Score::int>rcm.\"F_START\"::int and x.F_Score::int<=\"F_END\"::int Left join SAI_ABHIJITH348_DB.Maplemonk.SAI_ABHIJITH348_DB_next_purchase np On np.last_product = x.last_product ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SAI_ABHIJITH348_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        