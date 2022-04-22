{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table Almowear_DB.MAPLEMONK.Sales_Cost_Source as with orders as ( select date(FI.order_timestamp) Date ,FI.FINAL_UTM_CHANNEL Channel ,sum(FI.net_sales) Gross_Sales ,count(distinct FI.order_id) Orders ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.customer_id end)) as New_Customers ,count(distinct(case when lower(FI.customer_flag) = \'repeated\' then FI.customer_id end)) as Repeat_Customers ,sum(FI.discount) DISCOUNT ,sum(case when lower(FI.customer_flag) = \'new\' then FI.DISCOUNT end) as New_Customer_DISCOUNT ,sum(FI.mrp) MRP_Sales from fact_items FI where FI.source not in (\'Amazon\') group by 1,2 ), spend as (select date,channel, sum(spend) as spend from Almowear_DB.MAPLEMONK.MARKETING_CONSOLIDATED_AW group by 1,2 ) select coalesce(fi.Date,MC.date) as date, coalesce(FI.Channel, MC.channel) as channel, Gross_Sales, Orders, New_Customer_Orders, New_Customers, Repeat_Customers, DISCOUNT, New_Customer_DISCOUNT, MRP_Sales, spend as marketing_spend from orders FI full outer join spend MC on FI.Date = MC.date and FI.Channel = MC.channel ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from ALMOWEAR_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        