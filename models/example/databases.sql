{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table Almowear_DB.MAPLEMONK.Sales_Cost_Source as with orders as ( select date(FI.order_date) Date ,FI.SOURCE Channel ,ifnull(sum(case when lower(order_status) not in (\'cancelled\') then FI.selling_price end),0) Gross_Sales ,count(distinct case when lower(order_status) not in (\'cancelled\') then FI.order_id end) Orders ,count(distinct(case when lower(order_status) not in (\'cancelled\') and lower(FI.new_customer_flag) = 1 then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(order_status) not in (\'cancelled\') and lower(FI.new_customer_flag) = 1 then FI.customer_id end)) as New_Customers ,count(distinct case when lower(order_status) not in (\'cancelled\') then FI.customer_id_final end) as Unique_Customers ,count(distinct(case when lower(FI.new_customer_flag) = 0 and lower(order_status) not in (\'cancelled\') then FI.customer_id_final end)) as Repeat_Customers ,ifnull(sum(case when lower(order_status) not in (\'cancelled\') then FI.mrp_sales end),0) MRP_Sales ,ifnull(sum(case when lower(order_status) not in (\'cancelled\') then FI.mrp_sales end),0)-ifnull(sum(case when lower(order_status) not in (\'cancelled\') then FI.selling_price end),0) DISCOUNT ,sum(case when lower(FI.new_customer_flag) = 1 and lower(order_status) not in (\'cancelled\') then ifnull(FI.mrp_sales,0) -ifnull(FI.selling_price,0) end) as New_Customer_DISCOUNT ,ifnull(sum(case when lower(order_status) not in (\'cancelled\') then FI.suborder_quantity end),0) Gross_QUANTITY ,ifnull(sum(case when is_refund=1 and lower(order_status) not in (\'cancelled\') then FI.suborder_quantity end),0) as Return_Quantity ,ifnull(sum(case when is_refund=1 and lower(order_status) not in (\'cancelled\') then ifnull(FI.selling_price,0) end),0) as Return_Value ,Gross_Sales - Return_Value as Net_Sales from SALES_CONSOLIDATED_AW FI where lower(FI.marketplace) in (\'shopify_india\') group by 1,2 ) , spend as (select date,channel, sum(spend) as spend from Almowear_DB.MAPLEMONK.MARKETING_CONSOLIDATED_AW group by 1,2 ) select coalesce(fi.Date,MC.date) as date, coalesce(FI.Channel, MC.channel) as channel, Gross_Sales, Orders, New_Customer_Orders, New_Customers, Unique_Customers, Repeat_Customers, DISCOUNT, New_Customer_DISCOUNT, MRP_Sales, Gross_QUANTITY, Return_Quantity, Return_Value, spend as marketing_spend, Net_Sales from orders FI full outer join spend MC on FI.Date = MC.date and FI.Channel = MC.channel;",
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
                        