{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table Almowear_DB.MAPLEMONK.Sales_Cost_Source as with orders as ( select date(FI.order_timestamp) Date ,FI.FINAL_UTM_CHANNEL Channel ,sum(FI.net_sales)+sum(FI.tax)+sum(FI.shipping_price)+sum(shipping_tax) Gross_Sales ,count(distinct FI.order_id) Orders ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.customer_id end)) as New_Customers ,count(distinct FI.customer_id) as Unique_Customers ,count(distinct(case when lower(FI.customer_flag) = \'repeated\' then FI.customer_id end)) as Repeat_Customers ,sum(FI.mrp) MRP_Sales ,sum(FI.mrp)-(sum(FI.net_sales)+sum(FI.tax)+sum(FI.shipping_price)+sum(shipping_tax)) DISCOUNT ,sum(case when lower(FI.customer_flag) = \'new\' then FI.mrp -(FI.net_sales+FI.tax+FI.shipping_price+shipping_tax) end) as New_Customer_DISCOUNT ,sum(FI.quantity) Gross_QUANTITY ,sum(case when is_refund=1 then FI.quantity end) as Return_Quantity ,sum(case when is_refund=1 then FI.net_sales+FI.tax+FI.shipping_price+shipping_tax end) as Return_Value from fact_items FI where FI.source not in (\'Amazon\') and order_status in (\'Shopify_Processed\') group by 1,2 ), spend as (select date,channel, sum(spend) as spend from Almowear_DB.MAPLEMONK.MARKETING_CONSOLIDATED_AW group by 1,2 ) select coalesce(fi.Date,MC.date) as date, coalesce(FI.Channel, MC.channel) as channel, Gross_Sales, Orders, New_Customer_Orders, New_Customers, Unique_Customers, Repeat_Customers, DISCOUNT, New_Customer_DISCOUNT, MRP_Sales, Gross_QUANTITY, Return_Quantity, Return_Value, spend as marketing_spend from orders FI full outer join spend MC on FI.Date = MC.date and FI.Channel = MC.channel ;",
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
                        