{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table SYMPHONY_DB.MAPLEMONK.Sales_Cost_Source as with orders as ( select date(FI.order_timestamp) Date ,FI.FINAL_UTM_CHANNEL Channel ,ifnull(sum(FI.net_sales),0)+ifnull(sum(FI.tax),0)+ifnull(sum(FI.shipping_price),0)+ifnull(sum(shipping_tax),0) Gross_Sales ,count(distinct FI.order_id) Orders ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.customer_id end)) as New_Customers ,count(distinct FI.customer_id) as Unique_Customers ,count(distinct(case when lower(FI.customer_flag) = \'repeated\' then FI.customer_id end)) as Repeat_Customers ,ifnull(sum(FI.quantity),0) Gross_QUANTITY ,ifnull(sum(case when is_refund=1 then FI.quantity end),0) as Return_Quantity ,ifnull(sum(case when is_refund=1 then ifnull(FI.net_sales,0)+ifnull(FI.tax,0)+ifnull(FI.shipping_price,0)+ifnull(shipping_tax,0) end),0) as Return_Value from SYMPHONY_DB.MAPLEMONK.FACT_ITEMS FI where FI.source not in (\'Amazon\') and order_status in (\'Shopify_Processed\') group by 1,2 ), spend as (select date,channel, sum(spend) as spend from SYMPHONY_DB.MAPLEMONK.MARKETING_CONSOLIDATED_SYMPHONY group by 1,2 ) select coalesce(fi.Date,MC.date) as date, coalesce(FI.Channel, MC.channel) as channel, Gross_Sales, Orders, New_Customer_Orders, New_Customers, Unique_Customers, Repeat_Customers, Gross_QUANTITY, Return_Quantity, Return_Value, spend as marketing_spend from orders FI full outer join spend MC on FI.Date = MC.date and FI.Channel = MC.channel;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SYMPHONY_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        