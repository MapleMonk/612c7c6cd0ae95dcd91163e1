{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table pomme_DB.MAPLEMONK.Sales_Cost_Source_amiko_intermediate as with orders as ( select FI.order_date::date Date ,FI.SHOP_NAME ,ifnull(sum(ifnull(FI.SELLING_PRICE,0)),0) Total_Sales ,ifnull(sum(ifnull(FI.SELLING_PRICE,0)),0) - ifnull(sum(case when lower(FI.final_status) in (\'cancelled\') then FI.SELLING_PRICE end),0) TOTAL_SALES_EXCL_CANCL_inter ,count(distinct FI.order_id) Total_Orders ,count(distinct FI.order_id) - count(distinct case when lower(FI.final_status) in (\'cancelled\') then FI.order_id end) Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.final_status) in (\'cancelled\') then FI.order_id end)) as New_Customer_Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) as Total_New_Customers ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.final_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end)) New_Customers_EXCL_CANCL ,count(distinct FI.customer_id_final) as TOTAL_Unique_Customers ,(count(distinct FI.customer_id_final) - count(distinct case when lower(FI.final_status) in (\'cancelled\') then FI.customer_id_final end)) as Unique_Customers_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) as Repeat_Customers ,(count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' and lower(FI.final_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end))) Repeat_Customers_EXCL_CANCL ,ifnull(sum(FI.discount),0) TOTAL_DISCOUNT ,(ifnull(sum(FI.discount),0) - ifnull(sum(case when lower(FI.final_status) in (\'cancelled\') then FI.discount end),0)) TOTAL_DISCOUNT_EXCL_CANCL ,ifnull(sum(FI.tax),0) TOTAL_TAX ,TOTAL_SALES_EXCL_CANCL_inter - total_tax as TOTAL_SALES_EXCL_CANCL ,(ifnull(sum(FI.tax),0) - ifnull(sum(case when lower(FI.final_status) in (\'cancelled\') then FI.tax end),0)) TAX_EXCL_CANCL ,ifnull(sum(FI.shipping_price),0) TOTAL_SHIPPING_PRICE ,(ifnull(sum(FI.shipping_price),0) - ifnull(sum(case when lower(FI.final_status) in (\'cancelled\') then FI.shipping_price end),0)) SHIPPING_PRICE_EXCL_CANCL ,ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount end),0) as New_Customer_Discount ,(ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount end),0) - ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' and lower(final_status) in (\'cancelled\') then FI.discount end),0)) as New_Customer_Discount_EXCL_CANCL ,ifnull(sum(FI.suborder_quantity),0) TOTAL_QUANTITY ,(ifnull(sum(FI.suborder_quantity),0) - ifnull(sum(case when lower(FI.final_status) in (\'cancelled\') then FI.suborder_quantity end),0)) QUANTITY_EXCL_CANCL ,sum(ifnull(FI.refunded_quantity,0)) as Return_Quantity ,sum(ifnull(refunded_amount,0)) as Return_Value ,count(distinct case when lower(final_status) in (\'cancelled\') then order_id end) Cancelled_Orders ,count(distinct case when lower(final_status) not in (\'cancelled\',\'refunded\') then order_id end) Net_Orders ,count(distinct case when lower(final_status) in (\'delivered\') then order_id end) Delivered_Orders ,count(distinct case when lower(order_status) in (\'returned\',\'rto\') or lower(final_status) in (\'returned\',\'rto\') then order_id end) Returned_Orders ,count(distinct case when manifest_date is not null and lower(order_status) not in (\'cancelled\') then order_id end) Dispatched_Orders ,count(distinct case when lower(order_status) in (\'shipped\',\'printed\',\'confirmed\',\'returned\',\'ready to dispatch\') then order_id end) Realised_Orders ,ifnull(sum(case when lower(final_status) in (\'delivered\') then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0) Delivered_Revenue ,ifnull(sum(case when lower(order_status) in (\'returned\',\'rto\') or lower(final_status) in (\'returned\',\'rto\') then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0) Returned_Revenue ,ifnull(sum(case when manifest_date is not null and lower(final_status) not in (\'cancelled\') then ifnull(FI.SELLING_PRICE,0) end),0) Dispatched_Revenue ,ifnull(sum(case when lower(order_status) in (\'shipped\',\'printed\',\'confirmed\',\'returned\',\'ready to dispatch\') then ifnull(FI.SELLING_PRICE,0) end),0) Realised_Revenue ,count(case when date_trunc(\'month\',acquisition_date)>=dateadd(month,-3,date_trunc(\'month\',order_date)) and date_trunc(\'month\',acquisition_date)<date_trunc(\'month\',order_date) then customer_id_final end) L3M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date)>=dateadd(month,-6,date_trunc(\'month\',order_date)) and date_trunc(\'month\',acquisition_date)<date_trunc(\'month\',order_date) then customer_id_final end) L6M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date)>=dateadd(month,-3,date_trunc(\'month\',order_date)) and date_trunc(\'month\',acquisition_date)<date_trunc(\'month\',order_date) then customer_id_final end) L12M_Customers_Retained ,sum(case when lower(FI.new_customer_flag_month) = \'repeat\' then ifnull(FI.selling_price,0) end) Repeat_Customer_Revenue from pomme_DB.MAPLEMONK.SALES_CONSOLIDATED_amiko FI where lower(marketplace) like any (\'%woocommerce%\') group by 1,2 ), spend as (select date ,channel ,case when account in (\'Facebook Amiko\', \'Google Amiko\') then \'Shopify_Amiko\' end as Shop_Name ,sum(spend) as spend from pomme_DB.MAPLEMONK.MARKETING_CONSOLIDATED_amiko group by 1,2,3 ) select coalesce(fi.Date,MC.date) as date, MC.channel as channel, coalesce(FI.Shop_Name, MC.Shop_Name) as Marketplace, Total_Sales, TOTAL_SALES_EXCL_CANCL, Total_Orders, Orders_EXCL_CANCL, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, Return_Quantity, Return_Value, Cancelled_Orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, spend as marketing_spend, Repeat_Customer_Revenue from orders FI full outer join spend MC on FI.Date = MC.date and FI.Shop_name=MC.Shop_name ; Create or replace table pomme_DB.MAPLEMONK.SALES_COST_SOURCE_amiko as select coalesce(a.date, b.date) as date, a.channel as channel, coalesce(a.marketplace, b.shop_name) as Marketplace, Total_Sales, TOTAL_SALES_EXCL_CANCL, Total_Orders, Orders_EXCL_CANCL, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, RETURN_QUANTITY, RETURN_VALUE, Cancelled_Orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, marketing_spend, Repeat_Customer_Revenue, ifnull(b.customers,0) as MC_MP_Customer_Till_Date, ifnull(b.gross_sales,0) as MC_MP_Sales_Till_Date from pomme_DB.MAPLEMONK.Sales_Cost_Source_amiko_intermediate a full outer join (select date ,shop_name ,sum(gross_sales) over (partition by shop_name order by date asc rows between unbounded preceding and current row) gross_sales ,sum(customers) over (partition by shop_name order by date asc rows between unbounded preceding and current row) customers from ( select B.date, B.shop_name , sum(ifnull(selling_price,0)) gross_sales, count(distinct case when new_customer_flag = \'New\' then customer_id_final end) customers from pomme_db.maplemonk.sales_consolidated_amiko A full outer join (select * from (select distinct order_date::date date from pomme_db.maplemonk.sales_consolidated_amiko X) cross join (select distinct shop_name from pomme_db.maplemonk.sales_consolidated_amiko) Y) B on A.order_date::date=B.date AND A.SHOP_NAME=B.SHOP_NAME group by B.date, B.shop_name order by B.date desc ) order by date desc ) b on a.Date = b.date and a.marketplace=b.Shop_name order by 1 desc ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Pomme_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        