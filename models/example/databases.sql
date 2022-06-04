{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table FABALLEY_DB.MAPLEMONK.Sales_Cost_Source_Faballey as with orders as ( select order_date::date As ORDER_DATE ,COUNT(distinct order_id) AS Total_Orders ,(count(distinct order_id) - COUNT(distinct case when lower(order_status) in (\'cancelled\') and return_flag =0 then order_id end)) AS Gross_Orders ,COUNT(distinct case when lower(order_status) not in (\'cancelled\') and return_flag =0 then order_id end) AS Net_Orders ,count(distinct case when lower(order_status) in (\'cancelled\') and return_flag =0 then order_id end) AS Cancelled_Orders ,ifnull(sum(selling_price_inr),0) as Total_Sales_INR ,((ifnull(sum(selling_price_inr),0)) - (ifnull(sum(case when lower(Item_Status) in (\'cancelled\') and return_flag =0 then ifnull(selling_price_inr,0) end),0))) AS Gross_Sales_Inr ,ifnull(SUM(case when lower(Item_Status) not in (\'cancelled\') and return_flag =0 then ifnull(selling_price_inr,0) end),0) As Net_Sales_inr ,(ifnull(sum(case when lower(Item_Status) in (\'cancelled\') and return_flag =0 then ifnull(selling_price_inr,0) end),0)) AS Cancelled_Value_inr ,(ifnull(sum(case when return_flag =1 then ifnull(selling_price_inr,0) end),0)) AS Return_Value_inr ,ifnull(SUM(case when lower(Item_Status) not in (\'cancelled\') and return_flag =0 then discount_inr end),0) as Discount_On_Net_Sales_inr ,case when ifnull(SUM(case when lower(Item_Status) not in (\'cancelled\') and return_flag =0 then ifnull(selling_price_inr,0) end),0)=0 then 0 else (ifnull(SUM(case when lower(Item_Status) not in (\'cancelled\') and return_flag =0 then discount_inr end),0)/ifnull(SUM(case when lower(Item_Status) not in (\'cancelled\') and return_flag =0 then ifnull(selling_price_inr,0) end),0)) end As Discount_Percent_On_Net_Sales_inr ,(ifnull(sum(case when return_flag =0 then discount_inr end),0) - ifnull(sum(case when lower(Item_Status) in (\'cancelled\') and return_flag = 0 then discount_inr end),0)) AS Discount_on_Gross_Sales_inr ,case when COUNT(distinct case when lower(Item_Status) not in (\'cancelled\') and return_flag =0 then order_id end)=0 then 0 else (ifnull(SUM(case when lower(Item_Status) not in (\'cancelled\') and return_flag =0 then ifnull(selling_price_inr,0) end),0)/COUNT(distinct case when lower(order_status) not in (\'cancelled\') and return_flag =0 then order_id end)) end As Net_AOV_INR ,count(distinct case when lower(new_customer_flag) = \'new\' and lower(order_status) not in (\'cancelled\') and return_flag =0 then order_id end) as Net_Orders_New ,ifnull(SUM(case when lower(new_customer_flag) = \'new\' and lower(Item_Status) not in (\'cancelled\') and return_flag =0 then discount_inr end),0) as Discount_On_Net_Sales_New_inr ,count(distinct case when lower(order_status) not in (\'cancelled\') and return_flag =0 and lower(new_customer_flag) = \'new\' then customer_id end) as New_Customers ,count(distinct case when lower(order_status) not in (\'cancelled\') and return_flag =0 and lower(new_customer_flag) = \'Repeat\' then customer_id end) as Repeat_Customers from FABALLEY_DB.MAPLEMONK.Fact_item_Website_FabAlley FI group by 1 ), spend as (select date, sum(spend_inr) as spend from FABALLEY_DB.MAPLEMONK.MARKETING_CONSOLIDATED_FABALLEY group by 1 order by 1 desc ) select coalesce(fi.ORDER_DATE,MC.date) as date, Total_Orders, Gross_Orders, Net_Orders, Cancelled_Orders, Total_Sales_INR, Gross_Sales_INR, Net_Sales_INR, Cancelled_Value_INR, Return_Value_INR, Discount_On_Net_Sales_INR, Discount_on_Gross_Sales_INR, Net_AOV_INR, Net_Orders_New, Discount_On_Net_Sales_New_INR, New_Customers, Repeat_Customers, spend as marketing_spend from orders FI full outer join spend MC on FI.ORDER_DATE = MC.date;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from FABALLEY_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        