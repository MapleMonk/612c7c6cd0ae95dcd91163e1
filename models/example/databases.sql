{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table RPSG_DB.MAPLEMONK.Sales_Cost_Source_DRV_intermediate as with orders as ( select date(FI.order_date) Date ,FI.GA_CHANNEL Channel ,FI.SHOP_NAME ,ifnull(sum(fi.SELLING_PRICE),0) Total_Sales ,ifnull(sum(fi.SELLING_PRICE),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.SELLING_PRICE end),0) TOTAL_SALES_EXCL_CANCL ,count(distinct FI.order_id) Total_Orders ,count(distinct FI.order_id) - count(distinct case when lower(FI.order_status) in (\'cancelled\') then FI.order_id end) Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') then FI.order_id end)) as New_Customer_Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) as Total_New_Customers ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end)) New_Customers_EXCL_CANCL ,count(distinct FI.customer_id_final) as TOTAL_Unique_Customers ,(count(distinct FI.customer_id_final) - count(distinct case when lower(FI.order_status) in (\'cancelled\') then FI.customer_id_final end)) as Unique_Customers_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) as Repeat_Customers ,(count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' and lower(FI.order_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end))) Repeat_Customers_EXCL_CANCL ,ifnull(sum(FI.discount),0) TOTAL_DISCOUNT ,(ifnull(sum(FI.discount),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.discount end),0)) TOTAL_DISCOUNT_EXCL_CANCL ,ifnull(sum(FI.tax),0) TOTAL_TAX ,(ifnull(sum(FI.tax),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.tax end),0)) TAX_EXCL_CANCL ,ifnull(sum(FI.shipping_price),0) TOTAL_SHIPPING_PRICE ,(ifnull(sum(FI.shipping_price),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.shipping_price end),0)) SHIPPING_PRICE_EXCL_CANCL ,ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount end),0) as New_Customer_Discount ,(ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount end),0) - ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' and lower(order_status) in (\'cancelled\') then FI.discount end),0)) as New_Customer_Discount_EXCL_CANCL ,ifnull(sum(FI.suborder_quantity),0) TOTAL_QUANTITY ,(ifnull(sum(FI.suborder_quantity),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.suborder_quantity end),0)) QUANTITY_EXCL_CANCL ,ifnull(sum(case when FI.return_flag=1 then FI.suborder_quantity end),0) as Return_Quantity ,ifnull(sum(case when FI.return_flag=1 then ifnull(FI.SELLING_PRICE,0) end),0) as Return_Value ,count(distinct case when lower(order_status) in (\'cancelled\') then order_id end) Cancelled_Orders ,count(distinct case when lower(order_status) not in (\'cancelled\') and return_flag=0 then order_id end) Net_Orders ,count(distinct case when lower(shipping_status) in (\'delivered\') and lower(order_status) not in (\'cancelled\', \'open\',\'confirmed\') then order_id end) Delivered_Orders ,ifnull(sum(case when lower(shipping_status) in (\'delivered\') and lower(order_status) not in (\'cancelled\', \'open\',\'confirmed\') then FI.SELLING_PRICE end),0) Delivered_Revenue ,count(distinct case when manifest_date is not null and lower(order_status) not in (\'cancelled\') then order_id end) Dispatched_Orders ,ifnull(sum(case when manifest_date is not null and lower(order_status) not in (\'cancelled\') then FI.SELLING_PRICE end),0) Dispatched_Revenue from RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_DRV FI where lower(marketplace) like any (\'%shopify%\', \'%woocommerce%\') group by 1,2,3 ), spend as (select date ,channel ,case when account=\'Facebook Dr.Vaidyas CL H2T\' then \'Shopify_AyurvedicSource\' when account=\'Facebook Dr.Vaidyas\' then \'Shopify_DRV\' when account = \'Google Dr.Vaidyas\' then \'Shopify_DRV\' when account = \'Google Dr.Vaidyas 2\' then \'Shopify_DRV\' when account = \'Facebook Herbobuild\' then \'Shopify_Herbobuild\' end as Shop_Name ,sum(spend) as spend from RPSG_DB.MAPLEMONK.MARKETING_CONSOLIDATED_DRV group by 1,2,3 ) ,Users as (select ga_date date, channel, shop_name, sum(ga_users) users from RPSG_DB.MAPLEMONK.ga_sessions_consolidated_drv where not(lower(ga_campaign) like (\'%branding%\') and lower(ga_sourcemedium) like (\'%adyogi%\')) group by 1,2,3 ) select coalesce(fi.Date,MC.date,SC.date) as date, coalesce(FI.Channel, MC.channel, SC.channel) as channel, coalesce(FI.Shop_Name, MC.Shop_Name, SC.Shop_name) as Marketplace, Total_Sales, TOTAL_SALES_EXCL_CANCL, Total_Orders, Orders_EXCL_CANCL, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, Return_Quantity, Return_Value, Cancelled_Orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, spend as marketing_spend, SC.users as Traffic from orders FI full outer join spend MC on FI.Date = MC.date and FI.Channel = MC.channel and FI.Shop_name=MC.Shop_name full outer join Users SC on FI.Date=SC.Date and FI.Channel=SC.Channel and FI.Shop_name=SC.Shop_name ; Create or replace table RPSG_DB.MAPLEMONK.Sales_Cost_Source_DRV as select coalesce(a.date, b.date) as date, coalesce(a.channel, b.channel) as channel, coalesce(a.marketplace, b.shop_name) as Marketplace, Total_Sales, TOTAL_SALES_EXCL_CANCL, Total_Orders, Orders_EXCL_CANCL, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, Return_Quantity, Return_Value, Cancelled_Orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, marketing_spend, Traffic, ifnull(b.customers,0) as MC_MP_Customer_Till_Date, ifnull(b.gross_sales,0) as MC_MP_Sales_Till_Date from RPSG_DB.MAPLEMONK.Sales_Cost_Source_DRV_intermediate a full outer join (select date ,shop_name ,channel ,sum(gross_sales) over (partition by shop_name, channel order by date asc rows between unbounded preceding and current row) gross_sales ,sum(customers) over (partition by shop_name, channel order by date asc rows between unbounded preceding and current row) customers from ( select B.date, B.shop_name, B.ga_channel as channel, sum(ifnull(selling_price,0)) gross_sales, count(distinct case when new_customer_flag = \'New\' then customer_id_final end) customers from rpsg_db.maplemonk.sales_consolidated_drv A full outer join (select * from (select distinct order_date::date date from rpsg_db.maplemonk.sales_consolidated_drv X) cross join (select distinct shop_name, ga_channel from rpsg_db.maplemonk.sales_consolidated_drv) Y) B on A.Order_date::date=B.date AND A.SHOP_NAME=B.SHOP_NAME AND A.GA_CHANNEL=B.GA_CHANNEL group by B.date, B.shop_name, B.ga_channel order by B.date desc ) order by date desc ) b on a.Date = b.date and a.Channel = b.channel and a.marketplace=b.Shop_name; Create or replace table RPSG_DB.MAPLEMONK.Sales_Cost_Source_DRV as select a.* ,sum(MC_MP_Customer_Till_Date) over (partition by date) as Overall_Cust_Till_Date ,sum(MC_MP_Sales_Till_Date) over (partition by date) as Overall_Sales_Till_Date ,sum(MC_MP_Customer_Till_Date) over (partition by date, marketplace) as MP_Cust_Till_Date ,sum(MC_MP_Sales_Till_Date) over (partition by date, marketplace) as MP_Sales_Till_Date ,sum(MC_MP_Customer_Till_Date) over (partition by date, channel) as MC_Cust_Till_Date ,sum(MC_MP_Sales_Till_Date) over (partition by date, channel) as MC_Sales_Till_Date from RPSG_DB.MAPLEMONK.Sales_Cost_Source_DRV a;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from RPSG_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        