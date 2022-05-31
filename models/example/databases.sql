{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table XYXX_db.maplemonk.Marketplace_REPORT_XYXX AS with sales as ( select SHOP_NAME as Marketplace ,order_date::date As ORDER_DATE ,COUNT(distinct order_id) AS Total_Orders ,(count(distinct order_id) - COUNT(distinct case when lower(order_status) in (\'cancelled\') and return_flag =0 then order_id end)) AS Gross_Orders ,COUNT(distinct case when lower(order_status) not in (\'cancelled\') and return_flag =0 then order_id end) AS Net_Orders ,count(distinct case when lower(order_status) in (\'cancelled\') and return_flag =0 then order_id end) AS Cancelled_Orders ,(sum(ifnull(selling_price,0))+sum(ifnull(shipping_price,0))+sum(ifnull(discount,0))) as Total_Sales ,(sum(ifnull(selling_price,0))+sum(ifnull(shipping_price,0))+sum(ifnull(discount,0))) - (ifnull(sum(case when lower(order_status) in (\'cancelled\') and return_flag =0 then ifnull(selling_price,0)+ifnull(shipping_price,0)+ifnull(discount,0) end),0)) AS Gross_Sales ,(SUM(case when lower(order_status) not in (\'cancelled\') and return_flag =0 then ifnull(selling_price,0)+ifnull(shipping_price,0) end)) As Net_Sales ,(ifnull(sum(case when lower(order_status) in (\'cancelled\') and return_flag =0 then ifnull(selling_price,0)+ifnull(shipping_price,0)+ifnull(discount,0) end),0)) AS Cancelled_Value ,(ifnull(sum(case when return_flag =1 then ifnull(selling_price,0)+ifnull(shipping_price,0)+ifnull(discount,0) end),0)) AS Return_Value ,ifnull(SUM(case when lower(order_status) not in (\'cancelled\') and return_flag =0 then discount end),0) as Discount_On_Net_Sales ,case when ifnull(SUM(case when lower(order_status) not in (\'cancelled\') and return_flag =0 then ifnull(selling_price,0)+ifnull(shipping_price,0)+ifnull(discount,0) end),0)=0 then 0 else (ifnull(SUM(case when lower(order_status) not in (\'cancelled\') and return_flag =0 then discount end),0)/ifnull(SUM(case when lower(order_status) not in (\'cancelled\') and return_flag =0 then ifnull(selling_price,0)+ifnull(shipping_price,0)+ifnull(discount,0) end),0)) end As Discount_Percent_On_Net_Sales ,(ifnull(sum(case when return_flag =0 then discount end),0) - ifnull(sum(case when lower(order_status) in (\'cancelled\') and return_flag = 0 then discount end),0)) AS Discount_on_Gross_Sales ,case when COUNT(distinct case when lower(order_status) not in (\'cancelled\') and return_flag =0 then order_id end)=0 then 0 else (ifnull(SUM(case when lower(order_status) not in (\'cancelled\') and return_flag =0 then ifnull(selling_price,0)+ifnull(shipping_price,0) end),0)/COUNT(distinct case when lower(order_status) not in (\'cancelled\') and return_flag =0 then order_id end)) end As Net_AOV ,count(distinct case when lower(new_customer_flag) = \'new\' and lower(order_status) not in (\'cancelled\') and return_flag =0 then order_id end) as Net_Orders_New ,ifnull(SUM(case when lower(new_customer_flag) = \'new\' and lower(order_status) not in (\'cancelled\') and return_flag =0 then discount end),0) as Discount_On_Net_Sales_New ,count(distinct case when lower(order_status) not in (\'cancelled\') and return_flag =0 and lower(new_customer_flag) = \'new\' then customer_id end) as New_Customers ,count(distinct case when lower(order_status) not in (\'cancelled\') and return_flag =0 and lower(new_customer_flag) = \'Repeat\' then customer_id end) as Repeat_Customers ,count(distinct case when lower(order_status) not in (\'cancelled\') and return_flag =0 then customer_id end) AS Total_Customers from XYXX_DB.maplemonk.sales_consolidated_XYXX group by 1,2 ), marketing as ( select date,\'Shopify_India\' as Marketplace ,sum(spend) as Spend, avg(orders) orders,avg(Sales) Sales from XYXX_db.MAPLEMONK.MARKETING_CONSOLIDATED_XYXX a left join (select order_timestamp::date as order_date, count(distinct order_id) as orders, sum(total_sales) as Sales from XYXX_db.maplemonk.FACT_ITEMS_XYXX where shop_name like \'%hop%\' and final_utm_channel not in (\'Others\',\'Direct\') and order_status = \'Shopify_Processed\' group by 1 ) b on a.date = b.order_date group by 1,2 ), Amazon_ads as ( select date,\'Amazon\' as marketplace ,sum(spend) as spend , sum(sales) as amazon_ads_sales , sum(conversions) as amazon_ads_orders from XYXX_DB.MAPLEMONK.AMAZONADS_IN_MARKETING group by 1,2 ) select coalesce(s.marketplace,m.marketplace,a.marketplace) as marketplace ,coalesce(s.order_date,m.date,a.date) as order_date ,Total_Orders ,Gross_Orders ,Net_Orders ,Cancelled_Orders ,Total_Sales ,Gross_Sales ,Net_Sales ,Cancelled_Value ,Return_Value ,Discount_On_Net_Sales ,Discount_Percent_On_Net_Sales ,Discount_on_Gross_Sales ,Net_AOV ,Net_Orders_New ,Discount_On_Net_Sales_New ,New_Customers ,Repeat_Customers ,Total_Customers ,coalesce(m.spend,a.spend) as Spend ,coalesce(amazon_ads_sales,m.sales) as Ad_Sales ,coalesce(amazon_ads_orders,m.orders) as Ad_Orders from sales s full join marketing m on s.order_date = m.date and s.marketplace in (\'Shopify_India\') full join amazon_ads a on a.date = coalesce(s.order_date,m.date) and s.marketplace in (\'Amazon\') order by s.order_date desc;",
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
                        