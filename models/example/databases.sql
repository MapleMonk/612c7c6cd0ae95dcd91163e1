{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table RPSG_DB.MAPLEMONK.Sales_Cost_Source_Three60you_intermediate as with invoicedatemetrics as ( select try_to_date(FI.invoice_date) Invoice_Date ,\'OTHERS\' as CHANNEL ,upper(FI.SHOP_NAME) SHOP_NAME ,sum(ifnull((case when lower(order_status) not in (\'cancelled\') then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0)) Realised_Revenue from RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_THREE60 FI where lower(marketplace) like any (\'%three60%\') and not(lower(order_status) in (\'cancelled\') ) and invoice_date != \'\' group by 1,2,3 ), returnsales as ( select return_date::date return_date ,upper(channel) AS channel ,shop_name ,sum(total_return_amount) TOTAL_RETURN_AMOUNT ,sum(total_return_amount_excl_tax) TOTAL_RETURN_AMOUNT_EXCL_TAX ,sum(total_returned_quantity) TOTAL_RETURNED_QUANTITY from RPSG_DB.MAPLEMONK.easyecom_returns_summary_three60 where lower(company_name) like any (\'%herbolab%\',\'%dr vaidya%\') and lower(marketplace) like any (\'%three60%\') and return_date::date >= \'2024-05-01\' group by 1,2,3 order by 1 desc ) , orders as ( select FI.order_date::date Date ,upper(FI.pre_final_channel) Channel ,upper(FI.SHOP_NAME) SHOP_NAME ,count(distinct case when lower(payment_mode) = \'cod\' then order_id end) cod_Orders ,count(distinct case when lower(payment_mode) = \'prepaid\' then order_id end) prepaid_Orders ,ifnull(sum(ifnull(FI.SELLING_PRICE,0)),0) Total_Sales ,ifnull(sum(ifnull(FI.SELLING_PRICE,0)),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.SELLING_PRICE end),0) TOTAL_SALES_EXCL_CANCL ,ifnull(sum(case when not(lower(final_status) like any (\'%return%\',\'%rto%\',\'%cancel%\')) then FI.SELLING_PRICE end),0) TOTAL_SALES_EXCL_CANCL_RTO ,count(distinct case when not(lower(final_status) like any (\'%return%\',\'%rto%\',\'%cancel%\'))then order_id end ) as Orders_EXCL_CANCL_RTO ,count(distinct FI.order_id) Total_Orders ,count(distinct FI.order_id) - count(distinct case when lower(FI.order_status) in (\'cancelled\') then FI.order_id end) Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') then FI.order_id end)) as New_Customer_Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) as Total_New_Customers ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end)) New_Customers_EXCL_CANCL ,count(distinct FI.customer_id_final) as TOTAL_Unique_Customers ,(count(distinct FI.customer_id_final) - count(distinct case when lower(FI.order_status) in (\'cancelled\') then FI.customer_id_final end)) as Unique_Customers_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) as Repeat_Customers ,(count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' and lower(FI.order_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end))) Repeat_Customers_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.order_id end)) as Repeat_Orders ,sum(case when lower(FI.new_customer_flag) = \'repeat\' then ifnull(FI.selling_price,0) end) as Repeat_Revenue ,(count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then Fi.order_id end)) - count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' and lower(FI.order_status) in (\'cancelled\') and FI.return_flag = 0 then FI.order_id end))) Repeat_orders_EXCL_CANCL ,ifnull(sum(FI.discount_mrp),0) TOTAL_DISCOUNT ,(ifnull(sum(FI.discount_mrp),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.discount_mrp end),0)) TOTAL_DISCOUNT_EXCL_CANCL ,ifnull(sum(FI.tax),0) TOTAL_TAX ,(ifnull(sum(FI.tax),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.tax end),0)) TAX_EXCL_CANCL ,ifnull(sum(FI.shipping_price),0) TOTAL_SHIPPING_PRICE ,(ifnull(sum(FI.shipping_price),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.shipping_price end),0)) SHIPPING_PRICE_EXCL_CANCL ,ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount_mrp end),0) as New_Customer_Discount ,(ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount_mrp end),0) - ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' and lower(order_status) in (\'cancelled\') then FI.discount_mrp end),0)) as New_Customer_Discount_EXCL_CANCL ,ifnull(sum(FI.suborder_quantity),0) TOTAL_QUANTITY ,(ifnull(sum(FI.suborder_quantity),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.suborder_quantity end),0)) QUANTITY_EXCL_CANCL ,ifnull(sum(case when FI.return_flag=1 then FI.suborder_quantity end),0) as Return_Quantity ,ifnull(sum(case when FI.return_flag=1 then ifnull(FI.SELLING_PRICE,0) end),0) as Return_Value ,count(distinct case when FI.return_flag=1 then order_id end )as Return_Orders ,count(distinct case when lower(order_status) in (\'cancelled\') then order_id end) Cancelled_Orders ,count(distinct case when lower(order_status) not in (\'cancelled\') and return_flag=0 then order_id end) Net_Orders ,count(distinct case when lower(final_status) in (\'delivered\') then order_id end) Delivered_Orders ,count(distinct case when lower(final_status) in (\'returned\',\'rto\') or lower(final_status) in (\'returned\',\'rto\') then order_id end) Returned_Orders ,count(distinct case when manifest_date is not null and lower(order_status) not in (\'cancelled\') then order_id end) Dispatched_Orders ,Orders_EXCL_CANCL - Returned_Orders as Realised_Orders ,ifnull(sum(case when lower(final_status) in (\'delivered\') then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0) Delivered_Revenue ,ifnull(sum(case when lower(final_status) in (\'returned\',\'rto\') or lower(final_status) in (\'returned\',\'rto\') then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0) Returned_Revenue ,ifnull(sum(case when manifest_date is not null and lower(order_status) not in (\'cancelled\') then ifnull(FI.SELLING_PRICE,0) end),0) Dispatched_Revenue ,count(case when date_trunc(\'month\',acquisition_date::date)>=dateadd(month,-3,date_trunc(\'month\',order_date::date)) and date_trunc(\'month\',acquisition_date::date)<date_trunc(\'month\',order_date::date) then customer_id_final end) L3M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date::date)>=dateadd(month,-6,date_trunc(\'month\',order_date::date)) and date_trunc(\'month\',acquisition_date::date)<date_trunc(\'month\',order_date::date) then customer_id_final end) L6M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date::date)>=dateadd(month,-3,date_trunc(\'month\',order_date::date)) and date_trunc(\'month\',acquisition_date::date)<date_trunc(\'month\',order_date::date) then customer_id_final end) L12M_Customers_Retained ,sum(case when lower(FI.new_customer_flag_month) = \'repeat\' then ifnull(FI.selling_price,0) end) Repeat_Customer_Revenue from RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_THREE60 FI group by 1,2,3 ) ,spend as ( select Date, channel, Upper(account) as shop_name, sum(spend)Spend from rpsg_db.MAPLEMONK.marketing_consolidated_three60you where lower(channel) != \'amazon\' group by 1,2,3 order by 1 desc ) ,Users as ( select date, shop_name , \'others\' as channel , sum(ifnull(sessions,0))sessions, sum(totalusers) users from ( select to_date(date,\'YYYYMMDD\')Date, replace(split(sessionsourcemedium,\'/\')[0],\'\"\',\'\')source, replace(split(sessionsourcemedium,\'/\')[1],\'\"\',\'\')medium, sessionsourcemedium, landingpage, sessions, engagedsessions, totalusers, newusers, case when lower(landingpage) like any (\'%joint%\', \'%pain%\', \'%plus%\', \'%expert%\', \'%arthritis%\') then \'Three60plus\' else \'Three60\' end as shop_name from rpsg_db.maplemonk.ga4_three60you_sessions_by_landing_page ) group by 1,2,3 ), Allmetrics as ( select coalesce(fi.Date,MC.date,SC.date, RS.Return_Date, ID.invoice_date) as date, upper(coalesce(FI.Channel, MC.channel, SC.channel, RS.Channel, ID.Channel)) as channel, upper(coalesce(FI.Shop_Name, MC.Shop_Name, SC.Shop_name, RS.Shop_name, ID.Shop_name)) as Marketplace, cod_orders, prepaid_orders, Total_Sales, TOTAL_SALES_EXCL_CANCL, TOTAL_SALES_EXCL_CANCL_RTO, Total_Orders, Orders_EXCL_CANCL, Orders_EXCL_CANCL_RTO, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, Repeat_orders, Repeat_Revenue, Repeat_orders_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, RS.TOTAL_RETURNED_QUANTITY as Return_Quantity, RS.TOTAL_RETURN_AMOUNT_EXCL_TAX as Return_Value, Cancelled_Orders, return_Orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, Realised_Orders, ifnull(ID.Realised_Revenue,0)- ifnull(RS.TOTAL_RETURN_AMOUNT_EXCL_TAX,0) as Realised_Revenue, ifnull(ID.Realised_Revenue,0) Invoice_Amount_Excl_Tax, spend as marketing_spend, SC.users as Users, SC.Sessions as Traffic, Repeat_Customer_Revenue from orders FI full outer join spend MC on FI.Date = MC.date and lower(FI.Channel) = lower(MC.channel) and lower(FI.Shop_name)=lower(MC.Shop_name) full outer join Users SC on coalesce(FI.Date,MC.Date)=SC.Date and lower(coalesce(FI.Channel,MC.Channel))=lower(SC.Channel) and lower(coalesce(FI.Shop_name, MC.Shop_name))=lower(SC.Shop_name) full outer join returnsales RS on RS.return_date = coalesce(FI.Date,MC.Date,SC.Date) and lower(RS.Channel) = lower(coalesce(FI.Channel,MC.Channel, SC.Channel)) and lower(RS.Shop_name)=lower(coalesce(FI.Shop_name, MC.Shop_name, SC.Shop_name)) full outer join invoicedatemetrics ID on ID.invoice_date = coalesce(FI.Date,MC.Date,SC.Date, RS.return_date) and lower(coalesce(FI.Channel,MC.Channel, SC.Channel,RS.Channel))=lower(ID.Channel) and lower(coalesce(FI.Shop_name, MC.Shop_name, SC.Shop_name, RS.Shop_name))=lower(ID.Shop_name) ) select AM.Date as date, AM.channel channel, AM.Marketplace as Marketplace, sum(Total_Sales) Total_Sales, sum(cod_Orders) as cod_Orders, sum(prepaid_Orders) as prepaid_Orders, sum(TOTAL_SALES_EXCL_CANCL) TOTAL_SALES_EXCL_CANCL, sum(TOTAL_SALES_EXCL_CANCL_RTO) TOTAL_SALES_EXCL_CANCL_RTO, sum(Total_Orders) Total_Orders, sum(Orders_EXCL_CANCL) Orders_EXCL_CANCL, sum(Orders_EXCL_CANCL_RTO) Orders_EXCL_CANCL_RTO, sum(New_Customer_Orders) New_Customer_Orders, sum(New_Customer_Orders_EXCL_CANCL) New_Customer_Orders_EXCL_CANCL, sum(Total_New_Customers) Total_New_Customers, sum(New_Customers_EXCL_CANCL) New_Customers_EXCL_CANCL, sum(TOTAL_Unique_Customers) TOTAL_Unique_Customers, sum(Unique_Customers_EXCL_CANCL) Unique_Customers_EXCL_CANCL, sum(Repeat_Customers) Repeat_Customers, sum(Repeat_Customers_EXCL_CANCL) Repeat_Customers_EXCL_CANCL, sum(Repeat_orders) Repeat_orders, sum(Repeat_Revenue)Repeat_Revenue, sum(Repeat_orders_EXCL_CANCL) Repeat_orders_EXCL_CANCL, sum(TOTAL_DISCOUNT) TOTAL_DISCOUNT, sum(TOTAL_DISCOUNT_EXCL_CANCL) TOTAL_DISCOUNT_EXCL_CANCL, sum(TOTAL_TAX) TOTAL_TAX, sum(TAX_EXCL_CANCL) TAX_EXCL_CANCL, sum(TOTAL_SHIPPING_PRICE) TOTAL_SHIPPING_PRICE, sum(SHIPPING_PRICE_EXCL_CANCL) SHIPPING_PRICE_EXCL_CANCL, sum(New_Customer_DISCOUNT) New_Customer_DISCOUNT, sum(New_Customer_Discount_EXCL_CANCL) New_Customer_Discount_EXCL_CANCL, sum(TOTAL_QUANTITY) TOTAL_QUANTITY, sum(QUANTITY_EXCL_CANCL) QUANTITY_EXCL_CANCL, sum(Return_Quantity) Return_Quantity, sum(Return_Value) Return_Value, sum(Cancelled_Orders) Cancelled_Orders, sum(return_orders) return_orders, sum(Net_Orders) Net_Orders, sum(Delivered_Orders) Delivered_Orders, sum(Delivered_Revenue) Delivered_Revenue, sum(Dispatched_Orders) Dispatched_Orders, sum(Dispatched_Revenue) Dispatched_Revenue, sum(Realised_Orders) Realised_Orders, sum(Realised_Revenue) Realised_Revenue, sum(Invoice_Amount_Excl_Tax) Invoice_Amount_Excl_Tax, sum(marketing_spend) marketing_spend, sum(ifnull(Traffic,0)) Traffic, sum(ifnull(users,0)) Users, sum(Repeat_Customer_Revenue) as Repeat_Customer_Revenue from allmetrics AM group by 1,2,3 ; Create or replace table RPSG_DB.MAPLEMONK.SALES_COST_SOURCE_THREE60YOU as select coalesce(a.date, b.date) as date, upper(coalesce(b.channel, a.channel)) as channel, upper(coalesce(a.marketplace, b.shop_name)) as Marketplace, Total_Sales, cod_Orders, prepaid_Orders, Total_Sales/(1.13) Total_Sales_Ex_Tax, TOTAL_SALES_EXCL_CANCL, TOTAL_SALES_EXCL_CANCL_RTO, Orders_EXCL_CANCL_RTO, Total_Orders, Orders_EXCL_CANCL, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, Repeat_orders, Repeat_Revenue, Repeat_orders_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, Return_Quantity, Return_Value, Cancelled_Orders, return_orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, Realised_Orders, Invoice_Amount_Excl_Tax, Realised_Revenue, marketing_spend, Traffic, Users, Repeat_Customer_Revenue, ifnull(b.customers,0) as MC_MP_Customer_Till_Date, ifnull(b.gross_sales,0) as MC_MP_Sales_Till_Date from RPSG_DB.MAPLEMONK.SALES_COST_SOURCE_THREE60YOU_INTERMEDIATE a full outer join (select date ,upper(shop_name) shop_name ,upper(channel) channel ,sum(gross_sales) over (partition by shop_name, channel order by date asc rows between unbounded preceding and current row) gross_sales ,sum(customers) over (partition by shop_name, channel order by date asc rows between unbounded preceding and current row) customers from ( select B.date ,upper(B.shop_name) shop_name ,upper(B.pre_final_channel) as channel ,sum(ifnull(selling_price,0)) gross_sales ,count(distinct case when new_customer_flag = \'New\' then customer_id_final end) customers from rpsg_db.maplemonk.sales_consolidated_three60 A full outer join (select * from (select distinct order_date::date date from rpsg_db.maplemonk.sales_consolidated_three60 X) cross join (select distinct shop_name, pre_final_channel from rpsg_db.maplemonk.sales_consolidated_three60) Y) B on A.order_date::date=B.date AND lower(A.SHOP_NAME)=lower(B.SHOP_NAME) AND lower(A.pre_final_channel)=lower(B.pre_final_channel) where lower(A.marketplace) like any (\'%three60%\') group by B.date, upper(B.shop_name), upper(B.pre_final_channel) order by B.date desc ) order by date desc ) b on a.Date = b.date and lower(a.Channel) = lower(b.channel) and lower(a.marketplace)=lower(b.Shop_name) order by 1 desc ; create or replace table RPSG_DB.MAPLEMONK.Date_MP_MC_DIM_three60 AS select a.Acquisition_Month,upper(b.shop_name) shop_name,c.pre_final_channel from (select distinct date_trunc(\'month\', acquisition_date) Acquisition_Month from RPSG_DB.MAPLEMONK.sales_consolidated_three60 where acquisition_date is not null) a cross join (select distinct shop_name from RPSG_DB.MAPLEMONK.sales_consolidated_three60) b cross join (select distinct pre_final_channel from RPSG_DB.MAPLEMONK.sales_consolidated_three60 where ordeR_Date::date > \'2023-01-01\' and lower(shop_name) like any (\'%three60%\')) c where lower(b.shop_name) like any (\'%three60%\') and lower(shop_name) like any (\'%three60%\') order by 1 desc; Create or replace table RPSG_DB.MAPLEMONK.Sales_Cost_Source_three60you as with Preceding_New_Customers as ( select * ,sum(L3M_New_Customers) over (partition by acquisition_month) Overall_L3M_New_Customers ,sum(L6M_New_Customers) over (partition by acquisition_month) Overall_L6M_New_Customers ,sum(L12M_New_Customers) over (partition by acquisition_month) Overall_L12M_New_Customers ,sum(L3M_New_Customers) over (partition by acquisition_month,marketplace) MP_L3M_New_Customers ,sum(L6M_New_Customers) over (partition by acquisition_month,marketplace) MP_L6M_New_Customers ,sum(L12M_New_Customers) over (partition by acquisition_month,marketplace) MP_L12M_New_Customers ,sum(L3M_New_Customers) over (partition by acquisition_month,channel) MC_L3M_New_Customers ,sum(L6M_New_Customers) over (partition by acquisition_month,channel) MC_L6M_New_Customers ,sum(L12M_New_Customers) over (partition by acquisition_month,channel) MC_L12M_New_Customers from ( select Acquisition_Month ,upper(marketplace) marketplace ,upper(channel) channel ,ifnull(sum(New_Customers) over (partition by marketplace,channel order by acquisition_month rows between 3 preceding and 1 preceding),0) L3M_New_Customers ,ifnull(sum(New_Customers) over (partition by marketplace,channel order by acquisition_month rows between 6 preceding and 1 preceding),0) L6M_New_Customers ,ifnull(sum(New_Customers) over (partition by marketplace,channel order by acquisition_month rows between 12 preceding and 1 preceding),0) L12M_New_Customers from (Select distinct a.Acquisition_Month, upper(a.shop_name) marketplace, upper(a.pre_final_channel) channel, count(distinct case when lower(b.new_customer_flag) = \'new\' then b.customer_id_final end) New_Customers from RPSG_DB.MAPLEMONK.date_mp_mc_dim a left join RPSG_DB.MAPLEMONK.sales_consolidated_three60 b on a.Acquisition_Month = date_trunc(\'month\', b.acquisition_date) and lower(a.shop_name)=lower(b.shop_name) and lower(a.pre_final_channel)=lower(b.pre_final_channel) where lower(b.marketplace) like any (\'%three60%\') group by 1,2,3 order by 1 desc ) order by 1 desc ) order by 1 desc ) select coalesce(a.date,b.Acquisition_Month::date ) as date, upper(coalesce(a.channel, b.channel)) as pre_final_channel, case when upper(coalesce(a.channel, b.channel)) in ( \'ORGANIC\', \'DIRECT\', \'REFERRAL\', \'BRANDING\', \'WOOCOMMERCE\', \'NOT MAPPED\', \'RETENTION\', \'CRITEO\', \'FB\', \'SOCIAL\', \'GOOGLE\', \'CRM\', \'SNAPCHAT\') then upper(coalesce(a.channel, b.channel)) else \'OTHERS\' end channel, upper(coalesce(a.marketplace, b.marketplace)) as Marketplace, Total_Sales, cod_Orders, prepaid_Orders, Total_Sales_Ex_Tax, TOTAL_SALES_EXCL_CANCL, TOTAL_SALES_EXCL_CANCL_RTO, Total_Orders, Orders_EXCL_CANCL, Orders_EXCL_CANCL_RTO, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, Repeat_orders, Repeat_Revenue, Repeat_orders_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, Return_Quantity, Return_Value, Cancelled_Orders, return_orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, Realised_Orders, Invoice_Amount_Excl_Tax, Realised_Revenue, marketing_spend, Traffic, Users, Repeat_Customer_Revenue, MC_MP_Customer_Till_Date, MC_MP_Sales_Till_Date ,sum(MC_MP_Customer_Till_Date) over (partition by a.date) as Overall_Cust_Till_Date ,sum(MC_MP_Sales_Till_Date) over (partition by a.date) as Overall_Sales_Till_Date ,sum(MC_MP_Customer_Till_Date) over (partition by a.date, a.marketplace) as MP_Cust_Till_Date ,sum(MC_MP_Sales_Till_Date) over (partition by a.date, a.marketplace) as MP_Sales_Till_Date ,sum(MC_MP_Customer_Till_Date) over (partition by a.date, a.channel) as MC_Cust_Till_Date ,sum(MC_MP_Sales_Till_Date) over (partition by a.date, a.channel) as MC_Sales_Till_Date ,L3M_New_Customers as MC_MP_L3M_NEW_CUSTOMERS ,L6M_New_Customers as MC_MP_L6M_NEW_CUSTOMERS ,L12M_New_Customers as MC_MP_L12M_NEW_CUSTOMERS ,Overall_L3M_NEW_CUSTOMERS ,Overall_L6M_NEW_CUSTOMERS ,Overall_L12M_NEW_CUSTOMERS ,MP_L3M_NEW_CUSTOMERS ,MP_L6M_NEW_CUSTOMERS ,MP_L12M_NEW_CUSTOMERS ,MC_L3M_NEW_CUSTOMERS ,MC_L6M_NEW_CUSTOMERS ,MC_L12M_NEW_CUSTOMERS from RPSG_DB.MAPLEMONK.SALES_COST_SOURCE_THREE60YOU a full outer join Preceding_New_Customers b on date_trunc(\'month\',a.date)::date = b.Acquisition_Month::date and a.marketplace=b.marketplace and a.channel=b.channel order by 1 desc ; CREATE or REPLACE TABLE RPSG_DB.MAPLEMONK.TARGETS_three60 as with cte_targets as ( Select to_date(Month,\'mon/yyyy\') as MONTH_START_DATE ,metrics ,TRY_CAST(replace(TARGET,\',\',\'\') AS FLOAT) TARGET from RPSG_DB.MAPLEMONK.metrics_targets) select MONTH_START_DATE ,sum(ifnull(TARGET_BOOKED_REVENUE,0)) TARGET_BOOKED_REVENUE ,sum(ifnull(TARGET_BOOKED_REVENUE_EX_TAX,0)) TARGET_BOOKED_REVENUE_EX_TAX ,sum(ifnull(TARGET_TOTAL_ORDERS,0)) TARGET_TOTAL_ORDERS ,sum(ifnull(TARGET_SPEND,0)) TARGET_MARKETING_SPEND ,sum(ifnull(TARGET_DELIVERED_REVENUE,0)) TARGET_DELIVERED_REVENUE ,sum(ifnull(TARGET_RETENTION,0)) TARGET_RETENTION_REVENUE ,sum(ifnull(TARGET_USERS,0)) TARGET_USERS ,sum(ifnull(TARGET_TRAFFIC,0)) TARGET_TRAFFIC ,div0(sum(ifnull(TARGET_BOOKED_REVENUE,0)),sum(ifnull(TARGET_TOTAL_ORDERS,0))) TARGET_AOV ,div0(sum(ifnull(TARGET_DELIVERED_REVENUE,0)),sum(ifnull(TARGET_SPEND,0))) as TARGET_DELIVERED_ROAS ,div0(sum(ifnull(TARGET_BOOKED_REVENUE,0)),sum(ifnull(TARGET_SPEND,0))) as TARGET_BOOKED_ROAS ,div0(sum(ifnull(TARGET_TOTAL_ORDERS,0)),sum(ifnull(TARGET_TRAFFIC,0))) as TARGET_CONVERSION ,div0(sum(ifnull(TARGET_DELIVERED_REVENUE,0)),sum(ifnull(TARGET_BOOKED_REVENUE,0))) as TARGET_B2D ,div0(sum(ifnull(TARGET_DELIVERED_REVENUE,0)),sum(ifnull(TARGET_BOOKED_REVENUE_EX_TAX,0))) as TARGET_B2D_EX_Tax ,div0(sum(ifnull(TARGET_BOOKED_REVENUE_EX_TAX,0)),sum(ifnull(TARGET_SPEND,0))) as TARGET_BOOKED_ROAS_EX_TAX ,sum(ifnull(TARGET_INVOICE_AMOUNT_EX_TAX,0)) TARGET_INVOICE_AMOUNT_EX_TAX ,sum(ifnull(TARGET_RETURN_VALUE,0)) TARGET_RETURN_VALUE ,div0(sum(ifnull(TARGET_DELIVERED_REVENUE,0)),sum(ifnull(TARGET_INVOICE_AMOUNT_EX_TAX,0))) as TARGET_DISPATCH2DELIVER from ( SELECT MONTH_START_DATE , \"\'Booked Revenue\'\" TARGET_BOOKED_REVENUE , \"\'No of Orders\'\" TARGET_TOTAL_ORDERS , \"\'Spend\'\" TARGET_SPEND ,\"\'Delivered Revenue\'\" TARGET_DELIVERED_REVENUE ,\"\'Retention\'\" TARGET_RETENTION ,\"\'Users\'\" TARGET_USERS ,\"\'Traffic\'\" TARGET_TRAFFIC ,\"\'Booked Revenue Ex Tax\'\" TARGET_BOOKED_REVENUE_EX_TAX ,\"\'Dispatched Revenue\'\" TARGET_INVOICE_AMOUNT_EX_TAX ,\"\'Return amount\'\" TARGET_RETURN_VALUE FROM cte_targets PIVOT( SUM(TARGET) FOR METRICS IN (\'Booked Revenue\', \'No of Orders\', \'Spend\',\'Delivered Revenue\',\'Retention\',\'Users\',\'Traffic\', \'Booked Revenue Ex Tax\', \'Dispatched Revenue\', \'Return amount\') ) AS P ORDER BY MONTH_START_DATE) group by MONTH_START_DATE order by 1 desc;",
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
                        