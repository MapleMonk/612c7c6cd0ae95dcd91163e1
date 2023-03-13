{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE ghc_db.maplemonk.Customer_Retention_ghc AS SELECT year(Acq_date) Acquisition_Year, monthname(Acq_date) Acquisition_Month, last_day(Acq_date) Year_Month, count(distinct phone) total_customers, count(distinct case when last_day(Acq_date)=last_day(order_timestamp) then order_id end) total_orders, sum(case when last_day(Acq_date)=last_day(order_timestamp) then total_SALES end) total_sales, count(distinct case when last_day(Acq_date)<>last_day(order_timestamp) then phone end) mn_c, count(distinct case when last_day(dateadd(month,1,Acq_date))=last_day(order_timestamp) or last_day(dateadd(month,2,Acq_date))=last_day(order_timestamp) then phone end) m23_c, count(distinct case when last_day(dateadd(month,3,Acq_date))=last_day(order_timestamp) or last_day(dateadd(month,4,Acq_date))=last_day(order_timestamp) then phone end) m45_c, count(distinct case when last_day(dateadd(month,5,Acq_date))=last_day(order_timestamp) or last_day(dateadd(month,6,Acq_date))=last_day(order_timestamp) then phone end) m67_c, count(distinct case when last_day(dateadd(month,7,Acq_date))=last_day(order_timestamp) or last_day(dateadd(month,8,Acq_date))=last_day(order_timestamp) then phone end) m89_c, count(distinct case when last_day(dateadd(month,9,Acq_date))=last_day(order_timestamp) or last_day(dateadd(month,10,Acq_date))=last_day(order_timestamp) then phone end) m1011_c, count(distinct case when last_day(dateadd(month,11,Acq_date))=last_day(order_timestamp) or last_day(dateadd(month,12,Acq_date))=last_day(order_timestamp) then phone end) m1213_c from ( select phone, order_id, order_timestamp, total_SALES, (min(order_timestamp) over (partition by phone)) Acq_date from ghc_db.maplemonk.FACT_ITEMS_shopify_ghc where phone is not null and lower(order_status) not in (\'cancelled\') and is_refund <> 1 )res group by year(Acq_date), monthname(Acq_date), last_day(Acq_date) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GHC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        