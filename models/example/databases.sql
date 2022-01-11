{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.public.Customer_Cohort AS select distinct Year(min(order_timestamp) over (partition by customer_id)) as Acquisition_Year ,Month(min(order_timestamp) over (partition by customer_id)) as Acquisition_Month ,source as Channel,customer_id ,c.email,c.phone ,acquisition_product ,case when last_day(min(order_timestamp) over (partition by customer_id))=last_day(order_timestamp) then '1' when last_day(dateadd(month,1,min(order_timestamp) over (partition by customer_id)))=last_day(order_timestamp) then '2' when last_day(dateadd(month,2,min(order_timestamp) over (partition by customer_id)))=last_day(order_timestamp) then '3' when last_day(dateadd(month,3,min(order_timestamp) over (partition by customer_id)))=last_day(order_timestamp) then '4' when last_day(dateadd(month,4,min(order_timestamp) over (partition by customer_id)))=last_day(order_timestamp) then '5' when last_day(dateadd(month,5,min(order_timestamp) over (partition by customer_id)))=last_day(order_timestamp) then '6' when last_day(dateadd(month,6,min(order_timestamp) over (partition by customer_id)))=last_day(order_timestamp) then '7' when last_day(dateadd(month,7,min(order_timestamp) over (partition by customer_id)))=last_day(order_timestamp) then '8' when last_day(dateadd(month,8,min(order_timestamp) over (partition by customer_id)))=last_day(order_timestamp) then '9' when last_day(dateadd(month,9,min(order_timestamp) over (partition by customer_id)))=last_day(order_timestamp) then '10' when last_day(dateadd(month,10,min(order_timestamp) over (partition by customer_id)))=last_day(order_timestamp) then '11' when last_day(dateadd(month,11,min(order_timestamp) over (partition by customer_id)))=last_day(order_timestamp) then '12' else '12+' end AS Month ,product_name product from maplemonk.public.FACT_ITEMS fi left join maplemonk.PUBLIC.shopifyindia_customers c on fi.customer_id = c.id where customer_id is not null and order_timestamp is not null; CREATE OR REPLACE table maplemonk.public.Customer_Retention AS select year(Acq_date) Acquisition_Year,monthname(Acq_date) Acquisition_Month,last_day(Acq_date) Year_Month,Acquisition_Product ,count(distinct customer_id) total_customers ,count(distinct case when last_day(Acq_date)=last_day(order_timestamp) then order_id end) total_orders ,sum(case when last_day(Acq_date)=last_day(order_timestamp) then sales end) total_sales ,count(distinct case when last_day(Acq_date)<>last_day(order_timestamp) then customer_id end) mn_c ,count(distinct case when last_day(Acq_date)<>last_day(order_timestamp) then order_id end) mn_o ,sum(case when last_day(Acq_date)<>last_day(order_timestamp) then sales end) mn_s ,count(distinct case when acquisition_product=product_name and last_day(Acq_date)<>last_day(order_timestamp) then customer_id end) product_mn_c ,count(distinct case when acquisition_product=product_name and last_day(Acq_date)<>last_day(order_timestamp) then order_id end) product_mn_o ,sum(case when acquisition_product=product_name and last_day(Acq_date)<>last_day(order_timestamp) then sales end) product_mn_s ,count(distinct case when last_day(dateadd(month,1,Acq_date))=last_day(order_timestamp) then customer_id end) m2_c ,count(distinct case when last_day(dateadd(month,2,Acq_date))=last_day(order_timestamp) then customer_id end) m3_c ,count(distinct case when last_day(dateadd(month,3,Acq_date))=last_day(order_timestamp) then customer_id end) m4_c ,count(distinct case when last_day(dateadd(month,4,Acq_date))=last_day(order_timestamp) then customer_id end) m5_c ,count(distinct case when last_day(dateadd(month,5,Acq_date))=last_day(order_timestamp) then customer_id end) m6_c ,count(distinct case when last_day(dateadd(month,6,Acq_date))=last_day(order_timestamp) then customer_id end) m7_c ,count(distinct case when last_day(dateadd(month,7,Acq_date))=last_day(order_timestamp) then customer_id end) m8_c ,count(distinct case when last_day(dateadd(month,8,Acq_date))=last_day(order_timestamp) then customer_id end) m9_c ,count(distinct case when last_day(dateadd(month,9,Acq_date))=last_day(order_timestamp) then customer_id end) m10_c ,count(distinct case when last_day(dateadd(month,10,Acq_date))=last_day(order_timestamp) then customer_id end) m11_c ,count(distinct case when last_day(dateadd(month,11,Acq_date))=last_day(order_timestamp) then customer_id end) m12_c ,count(distinct case when last_day(dateadd(month,1,Acq_date))=last_day(order_timestamp) then order_id end) m2_o ,count(distinct case when last_day(dateadd(month,2,Acq_date))=last_day(order_timestamp) then order_id end) m3_o ,count(distinct case when last_day(dateadd(month,3,Acq_date))=last_day(order_timestamp) then order_id end) m4_o ,count(distinct case when last_day(dateadd(month,4,Acq_date))=last_day(order_timestamp) then order_id end) m5_o ,count(distinct case when last_day(dateadd(month,5,Acq_date))=last_day(order_timestamp) then order_id end) m6_o ,count(distinct case when last_day(dateadd(month,6,Acq_date))=last_day(order_timestamp) then order_id end) m7_o ,count(distinct case when last_day(dateadd(month,7,Acq_date))=last_day(order_timestamp) then order_id end) m8_o ,count(distinct case when last_day(dateadd(month,8,Acq_date))=last_day(order_timestamp) then order_id end) m9_o ,count(distinct case when last_day(dateadd(month,9,Acq_date))=last_day(order_timestamp) then order_id end) m10_o ,count(distinct case when last_day(dateadd(month,10,Acq_date))=last_day(order_timestamp) then order_id end) m11_o ,count(distinct case when last_day(dateadd(month,11,Acq_date))=last_day(order_timestamp) then order_id end) m12_o ,sum(case when last_day(dateadd(month,1,Acq_date))=last_day(order_timestamp) then sales end) m2_s ,sum(case when last_day(dateadd(month,2,Acq_date))=last_day(order_timestamp) then sales end) m3_s ,sum(case when last_day(dateadd(month,3,Acq_date))=last_day(order_timestamp) then sales end) m4_s ,sum(case when last_day(dateadd(month,4,Acq_date))=last_day(order_timestamp) then sales end) m5_s ,sum(case when last_day(dateadd(month,5,Acq_date))=last_day(order_timestamp) then sales end) m6_s ,sum(case when last_day(dateadd(month,6,Acq_date))=last_day(order_timestamp) then sales end) m7_s ,sum(case when last_day(dateadd(month,7,Acq_date))=last_day(order_timestamp) then sales end) m8_s ,sum(case when last_day(dateadd(month,8,Acq_date))=last_day(order_timestamp) then sales end) m9_s ,sum(case when last_day(dateadd(month,9,Acq_date))=last_day(order_timestamp) then sales end) m10_s ,sum(case when last_day(dateadd(month,10,Acq_date))=last_day(order_timestamp) then sales end) m11_s ,sum(case when last_day(dateadd(month,11,Acq_date))=last_day(order_timestamp) then sales end) m12_s ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,1,Acq_date))=last_day(order_timestamp) then customer_id end) product_m2_c ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,2,Acq_date))=last_day(order_timestamp) then customer_id end) product_m3_c ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,3,Acq_date))=last_day(order_timestamp) then customer_id end) product_m4_c ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,4,Acq_date))=last_day(order_timestamp) then customer_id end) product_m5_c ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,5,Acq_date))=last_day(order_timestamp) then customer_id end) product_m6_c ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,6,Acq_date))=last_day(order_timestamp) then customer_id end) product_m7_c ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,7,Acq_date))=last_day(order_timestamp) then customer_id end) product_m8_c ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,8,Acq_date))=last_day(order_timestamp) then customer_id end) product_m9_c ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,9,Acq_date))=last_day(order_timestamp) then customer_id end) product_m10_c ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,10,Acq_date))=last_day(order_timestamp) then customer_id end) product_m11_c ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,11,Acq_date))=last_day(order_timestamp) then customer_id end) product_m12_c ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,1,Acq_date))=last_day(order_timestamp) then order_id end) product_m2_o ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,2,Acq_date))=last_day(order_timestamp) then order_id end) product_m3_o ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,3,Acq_date))=last_day(order_timestamp) then order_id end) product_m4_o ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,4,Acq_date))=last_day(order_timestamp) then order_id end) product_m5_o ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,5,Acq_date))=last_day(order_timestamp) then order_id end) product_m6_o ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,6,Acq_date))=last_day(order_timestamp) then order_id end) product_m7_o ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,7,Acq_date))=last_day(order_timestamp) then order_id end) product_m8_o ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,8,Acq_date))=last_day(order_timestamp) then order_id end) product_m9_o ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,9,Acq_date))=last_day(order_timestamp) then order_id end) product_m10_o ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,10,Acq_date))=last_day(order_timestamp) then order_id end) product_m11_o ,count(distinct case when acquisition_product=product_name and last_day(dateadd(month,11,Acq_date))=last_day(order_timestamp) then order_id end) product_m12_o ,sum(case when acquisition_product=product_name and last_day(dateadd(month,1,Acq_date))=last_day(order_timestamp) then sales end) product_m2_s ,sum(case when acquisition_product=product_name and last_day(dateadd(month,2,Acq_date))=last_day(order_timestamp) then sales end) product_m3_s ,sum(case when acquisition_product=product_name and last_day(dateadd(month,3,Acq_date))=last_day(order_timestamp) then sales end) product_m4_s ,sum(case when acquisition_product=product_name and last_day(dateadd(month,4,Acq_date))=last_day(order_timestamp) then sales end) product_m5_s ,sum(case when acquisition_product=product_name and last_day(dateadd(month,5,Acq_date))=last_day(order_timestamp) then sales end) product_m6_s ,sum(case when acquisition_product=product_name and last_day(dateadd(month,6,Acq_date))=last_day(order_timestamp) then sales end) product_m7_s ,sum(case when acquisition_product=product_name and last_day(dateadd(month,7,Acq_date))=last_day(order_timestamp) then sales end) product_m8_s ,sum(case when acquisition_product=product_name and last_day(dateadd(month,8,Acq_date))=last_day(order_timestamp) then sales end) product_m9_s ,sum(case when acquisition_product=product_name and last_day(dateadd(month,9,Acq_date))=last_day(order_timestamp) then sales end) product_m10_s ,sum(case when acquisition_product=product_name and last_day(dateadd(month,10,Acq_date))=last_day(order_timestamp) then sales end) product_m11_s ,sum(case when acquisition_product=product_name and last_day(dateadd(month,11,Acq_date))=last_day(order_timestamp) then sales end) product_m12_s from ( select customer_id,order_id,acquisition_product,product_name,order_timestamp,sales,(min(order_timestamp) over (partition by customer_id)) Acq_date from maplemonk.public.FACT_ITEMS where customer_id is not null and source='Shopify' )res group by year(Acq_date),monthname(Acq_date),last_day(Acq_date),Acquisition_Product; CREATE OR REPLACE table maplemonk.public.Product_Migration AS select acquisition_product,product_name product,concat('purchase-',order_no) purchase ,count(distinct order_id) frequency ,max(orders) orders from ( select *,count(distinct order_id) over (partition by acquisition_product,order_no) orders from ( select customer_id,order_id,acquisition_product,product_name ,dense_rank() over (partition by acquisition_product,customer_id order by order_id) order_no from maplemonk.public.FACT_ITEMS where customer_id is not null and source='Shopify' )res1 )res2 group by acquisition_product,product_name,concat('purchase-',order_no) UNION ALL select acquisition_product,product_name,'all purchases',count(distinct order_id) Frequency,max(orders) Orders from ( select customer_id,order_id,acquisition_product,product_name,order_timestamp ,count(distinct order_id) over (partition by acquisition_product) orders from maplemonk.public.FACT_ITEMS where customer_id is not null )res group by acquisition_product,product_name",
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
            