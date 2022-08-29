{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table RPSG_DB.MAPLEMONK.Sales_Cost_Source_DRV_intermediate as with orders as ( select date(FI.order_date) Date ,FI.GA_CHANNEL Channel ,FI.SHOP_NAME ,ifnull(sum(fi.SELLING_PRICE),0) Total_Sales ,ifnull(sum(fi.SELLING_PRICE),0) - ifnull(sum(case when lower(FI.final_status) in (\'cancelled\') then FI.SELLING_PRICE end),0) TOTAL_SALES_EXCL_CANCL ,count(distinct FI.order_id) Total_Orders ,count(distinct FI.order_id) - count(distinct case when lower(FI.final_status) in (\'cancelled\') then FI.order_id end) Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.final_status) in (\'cancelled\') then FI.order_id end)) as New_Customer_Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) as Total_New_Customers ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.final_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end)) New_Customers_EXCL_CANCL ,count(distinct FI.customer_id_final) as TOTAL_Unique_Customers ,(count(distinct FI.customer_id_final) - count(distinct case when lower(FI.final_status) in (\'cancelled\') then FI.customer_id_final end)) as Unique_Customers_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) as Repeat_Customers ,(count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' and lower(FI.final_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end))) Repeat_Customers_EXCL_CANCL ,ifnull(sum(FI.discount),0) TOTAL_DISCOUNT ,(ifnull(sum(FI.discount),0) - ifnull(sum(case when lower(FI.final_status) in (\'cancelled\') then FI.discount end),0)) TOTAL_DISCOUNT_EXCL_CANCL ,ifnull(sum(FI.tax),0) TOTAL_TAX ,(ifnull(sum(FI.tax),0) - ifnull(sum(case when lower(FI.final_status) in (\'cancelled\') then FI.tax end),0)) TAX_EXCL_CANCL ,ifnull(sum(FI.shipping_price),0) TOTAL_SHIPPING_PRICE ,(ifnull(sum(FI.shipping_price),0) - ifnull(sum(case when lower(FI.final_status) in (\'cancelled\') then FI.shipping_price end),0)) SHIPPING_PRICE_EXCL_CANCL ,ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount end),0) as New_Customer_Discount ,(ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount end),0) - ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' and lower(final_status) in (\'cancelled\') then FI.discount end),0)) as New_Customer_Discount_EXCL_CANCL ,ifnull(sum(FI.suborder_quantity),0) TOTAL_QUANTITY ,(ifnull(sum(FI.suborder_quantity),0) - ifnull(sum(case when lower(FI.final_status) in (\'cancelled\') then FI.suborder_quantity end),0)) QUANTITY_EXCL_CANCL ,ifnull(sum(case when FI.return_flag=1 then FI.suborder_quantity end),0) as Return_Quantity ,ifnull(sum(case when FI.return_flag=1 then ifnull(FI.SELLING_PRICE,0) end),0) as Return_Value ,count(distinct case when lower(final_status) in (\'cancelled\') then order_id end) Cancelled_Orders ,count(distinct case when lower(final_status) not in (\'cancelled\') and return_flag=0 then order_id end) Net_Orders ,count(distinct case when lower(final_status) in (\'delivered\') then order_id end) Delivered_Orders ,ifnull(sum(case when lower(final_status) in (\'delivered\') then FI.SELLING_PRICE end),0) Delivered_Revenue ,count(distinct case when manifest_date is not null and lower(order_status) not in (\'cancelled\') then order_id end) Dispatched_Orders ,ifnull(sum(case when manifest_date is not null and lower(final_status) not in (\'cancelled\') then FI.SELLING_PRICE end),0) Dispatched_Revenue ,count(case when date_trunc(\'month\',acquisition_date)>=dateadd(month,-3,date_trunc(\'month\',order_date)) and date_trunc(\'month\',acquisition_date)<date_trunc(\'month\',order_date) then customer_id_final end) L3M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date)>=dateadd(month,-6,date_trunc(\'month\',order_date)) and date_trunc(\'month\',acquisition_date)<date_trunc(\'month\',order_date) then customer_id_final end) L6M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date)>=dateadd(month,-3,date_trunc(\'month\',order_date)) and date_trunc(\'month\',acquisition_date)<date_trunc(\'month\',order_date) then customer_id_final end) L12M_Customers_Retained from RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_DRV FI where lower(marketplace) like any (\'%shopify%\', \'%woocommerce%\') group by 1,2,3 ), spend as (select date ,channel ,case when account=\'Facebook Dr.Vaidyas CL H2T\' then \'Shopify_AyurvedicSource\' when account=\'Facebook Dr.Vaidyas\' then \'Shopify_DRV\' when account = \'Google Dr.Vaidyas\' then \'Shopify_DRV\' when account = \'Google Dr.Vaidyas 2\' then \'Shopify_DRV\' when account = \'Facebook Herbobuild\' then \'Shopify_Herbobuild\' end as Shop_Name ,sum(spend) as spend from RPSG_DB.MAPLEMONK.MARKETING_CONSOLIDATED_DRV group by 1,2,3 ) ,Users as (select ga_date date, channel, shop_name, sum(ga_users) users from RPSG_DB.MAPLEMONK.ga_sessions_consolidated_drv where not(lower(ga_campaign) like (\'%branding%\') and lower(ga_sourcemedium) like (\'%adyogi%\')) group by 1,2,3 ) select coalesce(fi.Date,MC.date,SC.date) as date, coalesce(FI.Channel, MC.channel, SC.channel) as channel, coalesce(FI.Shop_Name, MC.Shop_Name, SC.Shop_name) as Marketplace, Total_Sales, TOTAL_SALES_EXCL_CANCL, Total_Orders, Orders_EXCL_CANCL, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, Return_Quantity, Return_Value, Cancelled_Orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, spend as marketing_spend, SC.users as Traffic from orders FI full outer join spend MC on FI.Date = MC.date and FI.Channel = MC.channel and FI.Shop_name=MC.Shop_name full outer join Users SC on FI.Date=SC.Date and FI.Channel=SC.Channel and FI.Shop_name=SC.Shop_name ; Create or replace table RPSG_DB.MAPLEMONK.SALES_COST_SOURCE_DRV as select coalesce(a.date, b.date) as date, coalesce(a.channel, b.channel) as channel, coalesce(a.marketplace, b.shop_name) as Marketplace, Total_Sales, TOTAL_SALES_EXCL_CANCL, Total_Orders, Orders_EXCL_CANCL, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, Return_Quantity, Return_Value, Cancelled_Orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, marketing_spend, Traffic, ifnull(b.customers,0) as MC_MP_Customer_Till_Date, ifnull(b.gross_sales,0) as MC_MP_Sales_Till_Date from RPSG_DB.MAPLEMONK.Sales_Cost_Source_DRV_intermediate a full outer join (select date ,shop_name ,channel ,sum(gross_sales) over (partition by shop_name, channel order by date asc rows between unbounded preceding and current row) gross_sales ,sum(customers) over (partition by shop_name, channel order by date asc rows between unbounded preceding and current row) customers from ( select B.date, B.shop_name, B.ga_channel as channel, sum(ifnull(selling_price,0)) gross_sales, count(distinct case when new_customer_flag = \'New\' then customer_id_final end) customers from rpsg_db.maplemonk.sales_consolidated_drv A full outer join (select * from (select distinct order_date::date date from rpsg_db.maplemonk.sales_consolidated_drv X) cross join (select distinct shop_name, ga_channel from rpsg_db.maplemonk.sales_consolidated_drv) Y) B on A.Order_date::date=B.date AND A.SHOP_NAME=B.SHOP_NAME AND A.GA_CHANNEL=B.GA_CHANNEL group by B.date, B.shop_name, B.ga_channel order by B.date desc ) order by date desc ) b on a.Date = b.date and a.Channel = b.channel and a.marketplace=b.Shop_name order by 1 desc ; create or replace table RPSG_DB.MAPLEMONK.Date_MP_MC_DIM AS select a.Acquisition_Month,b.shop_name,c.ga_channel from (select distinct date_trunc(\'month\', acquisition_date) Acquisition_Month from RPSG_DB.MAPLEMONK.sales_consolidated_drv where acquisition_date is not null) a cross join (select distinct shop_name from RPSG_DB.MAPLEMONK.sales_consolidated_drv) b cross join (select distinct ga_channel from RPSG_DB.MAPLEMONK.sales_consolidated_drv) c where lower(b.shop_name) like any (\'%shopify%\', \'%woocommerce%\') order by 1 desc; Create or replace table RPSG_DB.MAPLEMONK.Sales_Cost_Source_DRV as with Preceding_New_Customers as ( select * ,sum(L3M_New_Customers) over (partition by acquisition_month) Overall_L3M_New_Customers ,sum(L6M_New_Customers) over (partition by acquisition_month) Overall_L6M_New_Customers ,sum(L12M_New_Customers) over (partition by acquisition_month) Overall_L12M_New_Customers ,sum(L3M_New_Customers) over (partition by acquisition_month,marketplace) MP_L3M_New_Customers ,sum(L6M_New_Customers) over (partition by acquisition_month,marketplace) MP_L6M_New_Customers ,sum(L12M_New_Customers) over (partition by acquisition_month,marketplace) MP_L12M_New_Customers ,sum(L3M_New_Customers) over (partition by acquisition_month,channel) MC_L3M_New_Customers ,sum(L6M_New_Customers) over (partition by acquisition_month,channel) MC_L6M_New_Customers ,sum(L12M_New_Customers) over (partition by acquisition_month,channel) MC_L12M_New_Customers from ( select Acquisition_Month ,marketplace ,channel ,ifnull(sum(New_Customers) over (partition by marketplace,channel order by acquisition_month rows between 3 preceding and 1 preceding),0) L3M_New_Customers ,ifnull(sum(New_Customers) over (partition by marketplace,channel order by acquisition_month rows between 6 preceding and 1 preceding),0) L6M_New_Customers ,ifnull(sum(New_Customers) over (partition by marketplace,channel order by acquisition_month rows between 12 preceding and 1 preceding),0) L12M_New_Customers from (Select distinct a.Acquisition_Month, a.shop_name marketplace, a.ga_channel channel, count(distinct case when lower(b.new_customer_flag) = \'new\' then b.customer_id_final end) New_Customers from RPSG_DB.MAPLEMONK.date_mp_mc_dim a left join RPSG_DB.MAPLEMONK.sales_consolidated_drv b on a.Acquisition_Month = date_trunc(\'month\', b.acquisition_date) and a.shop_name=b.shop_name and a.ga_channel=b.ga_channel group by 1,2,3 order by 1 desc ) order by 1 desc ) order by 1 desc ) select a.* ,sum(MC_MP_Customer_Till_Date) over (partition by a.date) as Overall_Cust_Till_Date ,sum(MC_MP_Sales_Till_Date) over (partition by a.date) as Overall_Sales_Till_Date ,sum(MC_MP_Customer_Till_Date) over (partition by a.date, a.marketplace) as MP_Cust_Till_Date ,sum(MC_MP_Sales_Till_Date) over (partition by a.date, a.marketplace) as MP_Sales_Till_Date ,sum(MC_MP_Customer_Till_Date) over (partition by a.date, a.channel) as MC_Cust_Till_Date ,sum(MC_MP_Sales_Till_Date) over (partition by a.date, a.channel) as MC_Sales_Till_Date ,L3M_New_Customers as MC_MP_L3M_NEW_CUSTOMERS ,L6M_New_Customers as MC_MP_L6M_NEW_CUSTOMERS ,L12M_New_Customers as MC_MP_L12M_NEW_CUSTOMERS ,Overall_L3M_NEW_CUSTOMERS ,Overall_L6M_NEW_CUSTOMERS ,Overall_L12M_NEW_CUSTOMERS ,MP_L3M_NEW_CUSTOMERS ,MP_L6M_NEW_CUSTOMERS ,MP_L12M_NEW_CUSTOMERS ,MC_L3M_NEW_CUSTOMERS ,MC_L6M_NEW_CUSTOMERS ,MC_L12M_NEW_CUSTOMERS from RPSG_DB.MAPLEMONK.Sales_Cost_Source_DRV a full outer join Preceding_New_Customers b on date_trunc(\'month\',a.date) = b.Acquisition_Month and a.marketplace=b.marketplace and a.channel=b.channel order by 1 desc ; CREATE or REPLACE TABLE RPSG_DB.MAPLEMONK.TARGETS as with cte_targets as ( Select to_date(Month,\'mon/yyyy\') as MONTH_START_DATE ,metrics ,TRY_CAST(replace(TARGET,\',\',\'\') AS FLOAT) TARGET from RPSG_DB.MAPLEMONK.metrics_targets) select MONTH_START_DATE ,sum(ifnull(TARGET_BOOKED_REVENUE,0)) TARGET_BOOKED_REVENUE ,sum(ifnull(TARGET_TOTAL_ORDERS,0)) TARGET_TOTAL_ORDERS ,sum(ifnull(TARGET_SPEND,0)) TARGET_MARKETING_SPEND ,sum(ifnull(TARGET_DELIVERED_REVENUE,0)) TARGET_DELIVERED_REVENUE ,sum(ifnull(TARGET_RETENTION,0)) TARGET_RETENTION_REVENUE ,sum(ifnull(TARGET_TRAFFIC,0)) TARGET_TRAFFIC ,sum(ifnull(TARGET_BOOKED_REVENUE,0))/sum(ifnull(TARGET_TOTAL_ORDERS,0)) TARGET_AOV ,sum(ifnull(TARGET_DELIVERED_REVENUE,0))/sum(ifnull(TARGET_SPEND,0)) as TARGET_DELIVERED_ROAS ,sum(ifnull(TARGET_BOOKED_REVENUE,0))/sum(ifnull(TARGET_SPEND,0)) as TARGET_BOOKED_ROAS ,sum(ifnull(TARGET_TOTAL_ORDERS,0))/sum(ifnull(TARGET_TRAFFIC,0)) as TARGET_CONVERSION ,sum(ifnull(TARGET_DELIVERED_REVENUE,0))/sum(ifnull(TARGET_BOOKED_REVENUE,0)) as TARGET_B2D from ( SELECT MONTH_START_DATE , \"\'Booked Revenue\'\" TARGET_BOOKED_REVENUE , \"\'No of Orders\'\" TARGET_TOTAL_ORDERS , \"\'Spend\'\" TARGET_SPEND ,\"\'Delivered Revenue\'\" TARGET_DELIVERED_REVENUE ,\"\'Retention\'\" TARGET_RETENTION ,\"\'Traffic\'\" TARGET_TRAFFIC FROM cte_targets PIVOT( SUM(TARGET) FOR METRICS IN (\'Booked Revenue\', \'No of Orders\', \'Spend\',\'Delivered Revenue\',\'Retention\',\'Traffic\') ) AS P ORDER BY MONTH_START_DATE) group by MONTH_START_DATE order by 1 desc; CREATE or REPLACE TABLE RPSG_DB.MAPLEMONK.EXEUCTIVE_SNAPSHOT_DRV as select * from ( select a.* ,sum(ifnull(a.traffic,0)) over (partition by month(a.date),year(a.date) order by a.date asc rows between unbounded preceding and current row)*(datediff(day,date_trunc(\'month\',a.date),last_day(a.date))+1)/day(a.date) as Traffic_Trend ,sum(ifnull(a.total_sales,0)) over (partition by month(a.date),year(a.date) order by a.date asc rows between unbounded preceding and current row)*(datediff(day,date_trunc(\'month\',a.date),last_day(a.date))+1)/day(a.date) as Total_Sales_Trend ,sum(ifnull(a.total_orders,0)) over (partition by month(a.date),year(a.date) order by a.date asc rows between unbounded preceding and current row)*(datediff(day,date_trunc(\'month\',a.date),last_day(a.date))+1)/day(a.date) as Total_Orders_Trend ,C.TARGET_B2D*sum(ifnull(a.total_sales,0)) over (partition by month(a.date),year(a.date) order by a.date asc rows between unbounded preceding and current row)*(datediff(day,date_trunc(\'month\',a.date),last_day(a.date))+1)/day(a.date) as Delivered_Revenue_Trend ,sum(ifnull(a.MARKETING_SPEND,0)) over (partition by month(a.date),year(a.date) order by a.date asc rows between unbounded preceding and current row)*(datediff(day,date_trunc(\'month\',a.date),last_day(a.date))+1)/day(a.date) as MARKETING_SPEND_Trend ,sum(ifnull(a.traffic,0)) over (partition by month(a.date),year(a.date) order by a.date asc rows between unbounded preceding and current row) Traffic_MTD ,sum(ifnull(a.total_sales,0)) over (partition by month(a.date),year(a.date) order by a.date asc rows between unbounded preceding and current row) Total_Sales_MTD ,sum(ifnull(a.total_orders,0)) over (partition by month(a.date),year(a.date) order by a.date asc rows between unbounded preceding and current row) Total_Orders_MTD ,sum(ifnull(a.delivered_revenue,0)) over (partition by month(a.date),year(a.date) order by a.date asc rows between unbounded preceding and current row) Total_Delivered_Revenue_MTD ,sum(ifnull(a.MARKETING_SPEND,0)) over (partition by month(a.date),year(a.date) order by a.date asc rows between unbounded preceding and current row) Marketing_Spend_MTD ,b.Overall_L3M_Customers_Retained Overall_L3M_Customers_Retained ,b.Overall_L6M_Customers_Retained Overall_L6M_Customers_Retained ,b.Overall_L12M_Customers_Retained Overall_L12M_Customers_Retained ,C.TARGET_BOOKED_REVENUE ,C.TARGET_TOTAL_ORDERS ,C.TARGET_AOV ,C.TARGET_MARKETING_SPEND ,C.TARGET_DELIVERED_REVENUE ,C.TARGET_BOOKED_ROAS ,C.TARGET_DELIVERED_ROAS ,C.TARGET_B2D ,C.TARGET_RETENTION_REVENUE ,C.TARGET_TRAFFIC ,C.TARGET_CONVERSION ,case when C.TARGET_TRAFFIC =0 then 0 else TRAFFIC_TREND/C.TARGET_TRAFFIC end as Traffic_Trend_VS_Target ,case when C.TARGET_TOTAL_ORDERS =0 then 0 else Total_Orders_Trend/C.TARGET_TOTAL_ORDERS end as ORDERS_TREND_VS_Target ,case when C.TARGET_BOOKED_REVENUE =0 then 0 else Total_Sales_Trend/C.TARGET_BOOKED_REVENUE end as BOOKED_REVENUE_TREND_VS_Target ,case when C.TARGET_DELIVERED_REVENUE =0 then 0 else Delivered_Revenue_Trend/C.TARGET_DELIVERED_REVENUE end as DELIVERED_REVENUE_TREND_VS_Target ,case when C.TARGET_MARKETING_SPEND =0 then 0 else MARKETING_SPEND_Trend/C.TARGET_MARKETING_SPEND end as MARKETING_SPEND_TREND_VS_Target ,case when Total_Orders_Trend=0 then 0 else (Total_Sales_Trend/Total_Orders_Trend)/C.TARGET_AOV end as AOV_TREND_VS_TARGET ,case when TRAFFIC_TREND=0 then 0 else (Total_Orders_Trend/TRAFFIC_TREND)/C.TARGET_CONVERSION end as CONVERSION_TREND_VS_TARGET ,case when Total_Sales_Trend=0 then 0 else (Delivered_Revenue_Trend/Total_Sales_Trend)/C.TARGET_B2D end as B2D_TREND_VS_TARGET ,case when MARKETING_SPEND_Trend=0 then 0 else (Total_Sales_Trend/MARKETING_SPEND_Trend)/C.TARGET_BOOKED_ROAS end as BOOKED_ROAS_TREND_VS_TARGET ,case when MARKETING_SPEND_Trend=0 then 0 else (Delivered_Revenue_Trend/MARKETING_SPEND_Trend)/C.TARGET_DELIVERED_ROAS end as DELIVERED_ROAS_TREND_VS_TARGET from (select date, sum(Total_Sales) TOTAL_SALES, sum(TOTAL_SALES_EXCL_CANCL) TOTAL_SALES_EXCL_CANCL, sum(Total_Orders) TOTAL_ORDERS, sum(Orders_EXCL_CANCL) ORDERS_EXCL_CANCL, sum(New_Customer_Orders) NEW_CUSTOMER_ORDERS, sum(New_Customer_Orders_EXCL_CANCL) NEW_CUSTOMER_ORDERS_EXCL_CANCL, sum(Total_New_Customers) TOTAL_NEW_CUSTOMERS, sum(New_Customers_EXCL_CANCL) NEW_CUSTOMERS_EXCL_CANCL, sum(TOTAL_Unique_Customers) TOTAL_UNIQUE_CUSTOMERS, sum(Unique_Customers_EXCL_CANCL) UNIQUE_CUSTOMERS_EXCL_CANCL, sum(Repeat_Customers) REPEAT_CUSTOMERS, sum(Repeat_Customers_EXCL_CANCL) REPEAT_CUSTOMERS_EXCL_CANCL, sum(TOTAL_DISCOUNT) TOTAL_DISCOUNT, sum(TOTAL_DISCOUNT_EXCL_CANCL) TOTAL_DISCOUNT_EXCL_CANCL, sum(TOTAL_TAX) TOTAL_TAX, sum(TAX_EXCL_CANCL) TAX_EXCL_CANCL, sum(TOTAL_SHIPPING_PRICE) TOTAL_SHIPPING_PRICE, sum(SHIPPING_PRICE_EXCL_CANCL) SHIPPING_PRICE_EXCL_CANCL, sum(New_Customer_DISCOUNT) NEW_CUSTOMER_DISCOUNT, sum(New_Customer_Discount_EXCL_CANCL) NEW_CUSTOMER_DISCOUNT_EXCL_CANCL, sum(TOTAL_QUANTITY) TOTAL_QUANTITY, sum(QUANTITY_EXCL_CANCL) QUANTITY_EXCL_CANCL, sum(Return_Quantity) RETURN_QUANTITY, sum(Return_Value) RETURN_VALUE, sum(Cancelled_Orders) CANCELLED_ORDERS, sum(Net_Orders) NET_ORDERS, sum(Delivered_Orders) DELIVERED_ORDERS, sum(Delivered_Revenue) DELIVERED_REVENUE, sum(Dispatched_Orders) DISPATCHED_ORDERS, sum(Dispatched_Revenue) DISPATCHED_REVENUE, sum(marketing_spend) MARKETING_SPEND, sum(Traffic) TRAFFIC, sum(MC_MP_Customer_Till_Date) CUSTOMER_TILL_DATE, sum(MC_MP_Sales_Till_Date) SALES_TILL_DATE, sum(MC_MP_L3M_NEW_CUSTOMERS) L3M_NEW_CUSTOMERS, sum(MC_MP_L6M_NEW_CUSTOMERS) L6M_NEW_CUSTOMERS, sum(MC_MP_L12M_NEW_CUSTOMERS) L12M_NEW_CUSTOMERS from RPSG_DB.MAPLEMONK.Sales_Cost_Source_DRV group by 1 order by 1 desc) a left join (select order_date::date date ,count(case when date_trunc(\'month\',acquisition_date)>=dateadd(month,-3,date_trunc(\'month\',order_date)) and date_trunc(\'month\',acquisition_date)<date_trunc(\'month\',order_date) then customer_id_final end) Overall_L3M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date)>=dateadd(month,-6,date_trunc(\'month\',order_date)) and date_trunc(\'month\',acquisition_date)<date_trunc(\'month\',order_date) then customer_id_final end) Overall_L6M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date)>=dateadd(month,-12,date_trunc(\'month\',order_date)) and date_trunc(\'month\',acquisition_date)<date_trunc(\'month\',order_date) then customer_id_final end) Overall_L12M_Customers_Retained from RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_DRV where lower(shop_name) like any (\'%shopify%\', \'%woocommerce%\') group by 1 order by 1 desc) b on a.date=b.date left join RPSG_DB.MAPLEMONK.TARGETS c on date_trunc(\'month\',a.date)=c.MONTH_START_DATE );",
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
                        