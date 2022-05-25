{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table XYXX_DB.MAPLEMONK.Sales_Cost_Source as with orders as ( select date(FI.order_timestamp) Date ,FI.FINAL_UTM_CHANNEL Channel ,ifnull(sum(FI.gross_sales_before_tax),0) GROSS_SALES_BEFORE_TAX ,ifnull(sum(FI.total_sales),0) TOTAL_SALES ,count(distinct FI.order_id) Orders ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id end)) as New_Customers ,count(distinct FI.customer_id) as Unique_Customers ,count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id end)) as Repeat_Customers ,ifnull(sum(FI.discount_before_tax),0) DISCOUNT ,ifnull(sum(FI.tax),0) TAX ,ifnull(sum(FI.shipping_price),0) SHIPPING_PRICE ,sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount_before_tax end) as New_Customer_Discount ,ifnull(sum(FI.quantity),0) Gross_QUANTITY ,ifnull(sum(case when is_refund=1 then FI.quantity end),0) as Return_Quantity ,ifnull(sum(case when is_refund=1 then ifnull(FI.gross_sales_before_tax,0) end),0) as Return_Value ,count(distinct case when is_refund=0 and lower(order_status) in (\'cancelled\') then order_id end) Cancelled_Orders ,count(distinct case when lower(order_status) not in (\'cancelled\') then order_id end) Net_Orders from XYXX_DB.MAPLEMONK.FACT_ITEMS_XYXX FI where FI.source not in (\'Amazon\') and order_status in (\'Shopify_Processed\') group by 1,2 ), spend as (select date,channel, sum(spend) as spend from XYXX_DB.MAPLEMONK.MARKETING_CONSOLIDATED_XYXX group by 1,2 ) select coalesce(fi.Date,MC.date) as date, coalesce(FI.Channel, MC.channel) as channel, GROSS_SALES_BEFORE_TAX, Total_Sales Orders, New_Customer_Orders, New_Customers, Unique_Customers, Repeat_Customers, DISCOUNT, TAX, SHIPPING_PRICE, New_Customer_DISCOUNT, Gross_QUANTITY, Return_Quantity, Return_Value, Cancelled_Orders, Net_Orders, spend as marketing_spend from orders FI full outer join spend MC on FI.Date = MC.date and FI.Channel = MC.channel;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from XYXX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        