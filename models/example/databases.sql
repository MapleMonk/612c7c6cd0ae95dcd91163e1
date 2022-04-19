{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table Almowear_DB.MAPLEMONK.Sales_Cost_Source as select date(FI.order_timestamp) Date ,FI.FINAL_UTM_CHANNEL Channel ,sum(FI.net_sales) Gross_Sales ,count(distinct FI.order_id) Orders ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.customer_id end)) as New_Customers ,count(distinct(case when lower(FI.customer_flag) = \'repeated\' then FI.customer_id end)) as Repeat_Customers ,sum(FI.discount) DISCOUNT ,sum(case when lower(FI.customer_flag) = \'new\' then FI.DISCOUNT end) as New_Customer_DISCOUNT ,sum(FI.mrp) MRP_Sales ,sum(MC.spend) Marketing_Spend from fact_items FI left join Almowear_DB.MAPLEMONK.MARKETING_CONSOLIDATED_AW MC on date(FI.order_timestamp) = MC.date and FI.FINAL_UTM_CHANNEL = MC.channel where FI.source not in (\'Amazon\') group by 1,2 order by date desc",
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
                        