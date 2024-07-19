{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch_intermediate as with orders as ( select date(FI.order_timestamp::date) Date ,payment_channel ,sum(dto_quant) as dto_quantity ,sum(cancel_quant) as cancel_quantity ,sum(cancel_sales) as cancel_sales ,ifnull(sum(fi.net_sales),0) Total_Sales ,ifnull(sum(fi.gross_sales),0) Gross_Sales ,sum(case when webshopney = \'Web\' then ifnull(FI.gross_sales ,0) end)web_gross_sales ,sum(case when webshopney != \'Web\' then ifnull(FI.gross_sales ,0) end)app_gross_sales ,sum(case when lower(FI.customer_flag) = \'new\' and webshopney = \'Web\' then ifnull(FI.gross_sales ,0) end) as new_customer_revenue_web ,sum(case when lower(FI.customer_flag) = \'repeated\' and webshopney = \'Web\' then ifnull(FI.gross_sales,0) end) as repeat_customer_revenue_web ,sum(case when lower(FI.customer_flag) = \'new\' and webshopney != \'Web\' then ifnull(FI.gross_sales ,0) end) as new_customer_revenue_app ,sum(case when lower(FI.customer_flag) = \'repeated\' and webshopney != \'Web\' then ifnull(FI.gross_sales,0) end) as repeat_customer_revenue_app ,ifnull(sum(fi.Gross_sales),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.net_sales end),0) Gross_SALES_EXCL_CANCL ,count(distinct FI.order_id) Total_Orders ,count(distinct FI.order_id) - count(distinct case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.order_id end) Orders_EXCL_CANCL ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(FI.customer_flag) = \'new\' and webshopney = \'Web\' then FI.order_id end)) as New_Web_Customer_Orders ,count(distinct(case when lower(FI.customer_flag) = \'new\' and webshopney != \'Web\' then FI.order_id end)) as New_App_Customer_Orders ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.order_id end)) - count(distinct(case when lower(FI.customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.order_id end)) as New_Customer_Orders_EXCL_CANCL ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.customer_id end)) as Total_New_Customers ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.customer_id end)) - count(distinct(case when lower(FI.customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.customer_id end)) New_Customers_EXCL_CANCL ,count(distinct FI.customer_id) as TOTAL_Unique_Customers ,(count(distinct FI.customer_id) - count(distinct case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.customer_id end)) as Unique_Customers_EXCL_CANCL ,count(distinct(case when lower(FI.customer_flag) = \'repeat\' then FI.customer_id end)) as Repeat_Customers ,(count(distinct(case when lower(FI.customer_flag) = \'repeat\' then FI.customer_id end)) - count(distinct(case when lower(FI.customer_flag) = \'repeat\' and lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.customer_id end))) Repeat_Customers_EXCL_CANCL ,ifnull(sum(FI.discount),0) TOTAL_DISCOUNT ,sum(case when lower(webshopney) = \'web\' then ifnull(FI.discount ,0) end) as web_discount ,sum(case when lower(webshopney) != \'web\' then ifnull(FI.discount,0) end) as app_discount ,(ifnull(sum(FI.discount),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.discount end),0)) TOTAL_DISCOUNT_EXCL_CANCL ,ifnull(sum(FI.tax),0) TOTAL_TAX ,(ifnull(sum(FI.tax),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.tax end),0)) TAX_EXCL_CANCL ,ifnull(sum(FI.shipping_price),0) TOTAL_SHIPPING_PRICE ,(ifnull(sum(FI.shipping_price),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.shipping_price end),0)) SHIPPING_PRICE_EXCL_CANCL ,ifnull(sum(case when lower(FI.customer_flag) = \'new\' then FI.discount end),0) as New_Customer_Discount ,(ifnull(sum(case when lower(FI.customer_flag) = \'new\' then FI.discount end),0) - ifnull(sum(case when lower(FI.customer_flag) = \'new\' and lower(order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.discount end),0)) as New_Customer_Discount_EXCL_CANCL ,ifnull(sum(FI.quantity),0) TOTAL_QUANTITY ,(ifnull(sum(FI.quantity),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.quantity end),0)) QUANTITY_EXCL_CANCL ,ifnull(sum(case when FI.is_refund=1 then FI.quantity end),0) as Return_Quantity ,ifnull(sum(case when FI.is_refund=1 then ifnull(FI.net_sales,0) end),0) as Return_Value ,count(distinct case when FI.is_refund=0 and lower(order_status) in (\'cancelled\') then order_id end) Cancelled_Orders ,count(distinct case when lower(order_status) not in (\'cancelled\') and is_refund=0 then order_id end) Net_Orders ,count(distinct case when lower(webshopney) =\'web\' then order_name end) Website_Orders ,count(distinct case when lower(webshopney) =\'appbrew\' then order_name end) App_Orders from Snitch_db.maplemonk.FACT_ITEMS_SNITCH FI where lower(ifnull(discount_code,\'n\')) not like \'%eco%\' and lower(ifnull(discount_code,\'n\')) not like \'%influ%\' and order_name not in (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') and tags not in (\'FLITS_LOGICERP\') and app_id != \'110036811777\' group by 1,2 ), RTO_Data as ( select date(FI.RTO_DATE::date) Date ,payment_channel ,sum(rto_sales) rto_sales ,sum(rto_quant) rto_quantity from Snitch_db.maplemonk.FACT_ITEMS_SNITCH FI where lower(ifnull(discount_code,\'n\')) not like \'%eco%\' and lower(ifnull(discount_code,\'n\')) not like \'%influ%\' and order_name not in (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') and tags not in (\'FLITS_LOGICERP\') and app_id != \'110036811777\' and RTO_DATE is not null group by 1,2 ), DTO_Data as ( select date(FI.DTO_Date::date) Date ,payment_channel ,sum(dto_sales) dto_sales ,sum(dto_quant) dto_quantity from Snitch_db.maplemonk.FACT_ITEMS_SNITCH FI where lower(ifnull(discount_code,\'n\')) not like \'%eco%\' and lower(ifnull(discount_code,\'n\')) not like \'%influ%\' and order_name not in (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') and tags not in (\'FLITS_LOGICERP\') and app_id != \'110036811777\' and DTO_Date is not null group by 1,2 ), spend as (select date, sum(spend) as spend from Snitch_db.MAPLEMONK.MARKETING_CONSOLIDATED_SNITCH group by 1 ) select coalesce(fi.Date,MC.date,RD.Date,dd.date) as date, coalesce(fi.payment_channel,RD.payment_channel,dd.payment_channel) as payment_channel, Total_Sales, cancel_quantity, FI.Gross_Sales, FI.web_gross_sales, FI.app_gross_sales, FI.new_customer_revenue_web, FI.repeat_customer_revenue_web, FI.new_customer_revenue_app, FI.repeat_customer_revenue_app, Gross_SALES_EXCL_CANCL, Total_Orders, Orders_EXCL_CANCL, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, Return_Quantity, Return_Value, Cancelled_Orders, Net_Orders, Website_Orders, App_Orders, web_discount, app_discount, New_Web_Customer_Orders, New_App_Customer_Orders, RD.rto_sales, RD.RTO_Quantity, dd.dto_quantity, dd.dto_sales, FI.cancel_sales, div0(spend,count(1) over (partition by fi.date order by 1)) as marketing_spend from orders FI full outer join spend MC on FI.Date = MC.date full outer join RTO_Data RD on FI.Date = RD.date and FI.payment_channel = RD.payment_channel full outer join DTO_Data DD on FI.Date = DD.date and FI.payment_channel = DD.payment_channel ; create or replace table snitch_db.maplemonk.sales_cost_source_mtd as select c.date, c.payment_channel, c.mtd_gross_sales, c.mtd_total_orders, c.mtd_total_new_customers, c.mtd_total_discount, c.mtd_marketing_spend, d.mtd_gross_sales lmtd_gross_sales, d.mtd_total_orders lmtd_total_orders, d.mtd_total_new_customers lmtd_total_new_customers, d.mtd_total_discount lmtd_total_discount, d.mtd_marketing_spend lmtd_marketing_spend, c.rto_quantity as mtd_rto_quantity, c.dto_quantity as mtd_dto_quantity, c.cancel_quantity as mtd_cancel_quantity, d.rto_quantity as lmtd_rto_quantity, d.dto_quantity as lmtd_dto_quantity, d.cancel_quantity as lmtd_cancel_quantity from (select a.date, a.payment_channel, sum(b.gross_sales) mtd_gross_Sales, sum(b.total_orders) mtd_total_orders, sum(b.total_new_customers) mtd_total_new_customers, sum(b.total_discount) mtd_total_discount, sum(b.marketing_spend) mtd_marketing_spend, sum(a.rto_quantity) as rto_quantity, sum(a.dto_quantity) as dto_quantity, sum(a.cancel_quantity) as cancel_quantity from Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch_intermediate a left join (select date, payment_channel,gross_Sales,total_orders, total_new_customers, total_discount, marketing_spend from Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch_intermediate) b on a.date >= b.date and date_trunc(\'month\',a.date) = date_trunc(\'month\',b.date) and a.payment_channel = b.payment_channel group by 1,2) c left join (select a.date, a.payment_channel, sum(b.gross_sales) mtd_gross_Sales, sum(b.total_orders) mtd_total_orders, sum(b.total_new_customers) mtd_total_new_customers, sum(b.total_discount) mtd_total_discount, sum(b.marketing_spend) mtd_marketing_spend, sum(a.rto_quantity) as rto_quantity, sum(a.dto_quantity) as dto_quantity, sum(a.cancel_quantity) as cancel_quantity from Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch_intermediate a left join (select date, payment_channel,gross_Sales, total_orders, total_new_customers, total_discount, marketing_spend from Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch_intermediate) b on a.date >= b.date and date_trunc(\'month\',a.date) = date_trunc(\'month\',b.date) and a.payment_channel = b.payment_channel group by 1,2) d on d.date = dateadd(month,-1, c.date)::date and c.payment_channel = d.payment_channel ; create or replace table snitch_db.maplemonk.web_app_conversion_mtd as select c.date, c.mtd_web_Sessions, c.mtd_web_orders, c.mtd_appbrew_sessions, c.mtd_appbrew_orders, d.mtd_web_Sessions lmtd_web_Sessions, d.mtd_web_orders lmtd_web_orders, d.mtd_appbrew_sessions lmtd_appbrew_sessions, d.mtd_appbrew_orders lmtd_appbrew_orders from (select a.date, sum(ifnull(b.web_sessions,0)) mtd_web_Sessions, sum(ifnull(b.web_orders,0)) mtd_web_orders, sum(b.appbrew_Sessions) mtd_appbrew_sessions, sum(b.appbrew_orders) mtd_appbrew_orders from snitch_db.maplemonk.web_app_conversion_snitch a left join ( select date,web_sessions, web_orders, appbrew_Sessions, appbrew_orders from snitch_db.maplemonk.web_app_conversion_snitch app ) b on a.date >= b.date and date_trunc(\'month\',a.date) = date_trunc(\'month\',b.date) group by 1 ) c left join ( select a.date, sum(ifnull(b.web_sessions,0)) mtd_web_Sessions, sum(ifnull(b.web_orders,0)) mtd_web_orders, sum(b.appbrew_Sessions) mtd_appbrew_sessions, sum(b.appbrew_orders) mtd_appbrew_orders from snitch_db.maplemonk.web_app_conversion_snitch a left join ( select date, web_sessions, web_orders, appbrew_Sessions,appbrew_orders from snitch_db.maplemonk.web_app_conversion_snitch app ) b on a.date >= b.date and date_trunc(\'month\',a.date) = date_trunc(\'month\',b.date) group by 1 ) d on d.date = dateadd(month,-1, c.date)::date ; Create or replace table Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch as select a.date as date, a.payment_channel, a.rto_sales, a.RTO_Quantity, a.dto_sales, a.dto_quantity, a.cancel_sales, Total_Sales, A.GROSS_Sales, A.web_gross_sales, A.app_gross_sales, A.new_customer_revenue_web, A.repeat_customer_revenue_web, A.new_customer_revenue_app, A.repeat_customer_revenue_app, GROSS_SALES_EXCL_CANCL, a.Total_Orders, a.cancel_quantity, d.lmtd_rto_quantity, d.lmtd_dto_quantity, d.lmtd_cancel_quantity, d.mtd_rto_quantity, d.mtd_dto_quantity, d.mtd_cancel_quantity, Orders_EXCL_CANCL, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, Return_Quantity, Return_Value, Cancelled_Orders, Net_Orders, marketing_spend, ifnull(b.customers,0) as Total_Customer_Till_Date, ifnull(b.gross_sales,0) as Sales_Till_Date, div0(c.web_sessions, count(1) over (partition by a.date order by 1)) web_Sessions, div0(c.web_orders, count(1) over (partition by a.date order by 1)) web_orders, div0(c.appbrew_sessions, count(1) over (partition by a.date order by 1)) appbrew_Sessions, div0(c.appbrew_orders, count(1) over (partition by a.date order by 1)) appbrew_orders, mtd_gross_sales, mtd_total_orders, mtd_total_new_customers, mtd_total_discount, mtd_marketing_spend, lmtd_gross_sales, lmtd_total_orders, lmtd_total_new_customers, lmtd_total_discount, lmtd_marketing_spend, div0(mtd_web_Sessions, count(1) over (partition by a.date order by 1)) mtd_web_sessions, div0(mtd_web_orders, count(1) over (partition by a.date order by 1)) mtd_web_orders, div0(mtd_appbrew_sessions, count(1) over (partition by a.date order by 1)) mtd_app_sessions, div0(mtd_appbrew_orders, count(1) over (partition by a.date order by 1)) mtd_app_orders, div0(lmtd_web_Sessions, count(1) over (partition by a.date order by 1)) lmtd_web_Sessions, div0(lmtd_web_orders, count(1) over (partition by a.date order by 1)) lmtd_web_orders, div0(lmtd_appbrew_sessions, count(1) over (partition by a.date order by 1)) lmtd_appbrew_sessions, div0(lmtd_appbrew_orders, count(1) over (partition by a.date order by 1)) lmtd_appbrew_orders, f.target gross_Sales_target_month, g.target discount_target_month, h.target qty_per_order_target_month, i.target aov_target_month, j.target spends_target_month, k.target Discount_INR_target_month, l.target New_Customers_target_month, m.target Old_Customers_target_month, n.target Orders_target_month, o.target appbrew_orders_target_month, p.target web_orders_target_month, q.target appbrew_Sessions_target_month, r.target web_sessions_target_month, div0(right(a.date,2),right(last_day(a.date,\'month\'),2)) ratio, f.target::float*ratio gross_Sales_target_mtd, g.target::float discount_target_mtd, h.target::float qty_per_order_target_mtd, i.target::float aov_target_mtd, j.target::float*ratio spends_target_mtd, k.target::float*ratio Discount_INR_target_mtd, l.target::float*ratio New_Customers_target_mtd, m.target::float*ratio Old_Customers_target_mtd, n.target::float*ratio Orders_target_mtd, o.target::float*ratio app_orders_target_mtd, p.target::float*ratio web_orders_target_mtd, q.target::float*ratio app_Sessions_target_mtd, r.target::float*ratio web_sessions_target_mtd, Website_Orders, App_Orders, web_discount, app_discount, New_Web_Customer_Orders, New_App_Customer_Orders from Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch_intermediate a full outer join (select date ,payment_channel ,sum(gross_sales) over (partition by payment_channel order by date asc rows between unbounded preceding and current row) gross_sales ,sum(customers) over (partition by payment_channel order by date asc rows between unbounded preceding and current row) customers from ( select A.order_timestamp::date date, payment_channel, ifnull(sum(a.Gross_sales),0) - ifnull(sum(case when lower(a.order_status) in (\'cancelled\') or a.is_refund = 1 then a.gross_sales end),0) gross_sales, count(distinct case when customer_flag = \'New\' then customer_id end) customers from Snitch_db.maplemonk.FACT_ITEMS_SNITCH A group by A.order_timestamp::date, payment_channel order by A.order_timestamp::date, payment_channel desc ) order by date desc ) b on a.Date = b.date and a.payment_channel = b.payment_channel left join snitch_db.maplemonk.web_app_conversion_snitch c on a.date = c.date left join snitch_db.maplemonk.sales_cost_source_mtd d on a.date = d.date and a.payment_channel = d.payment_channel left join snitch_db.maplemonk.web_app_conversion_mtd e on a.date = e.date left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Gross Sales\') f on date_trunc(\'month\',a.date) = f.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Discount\') g on date_trunc(\'month\',a.date) = g.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Qty Per Order\') h on date_trunc(\'month\',a.date) = h.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'AOV\') i on date_trunc(\'month\',a.date) = i.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Spends\') j on date_trunc(\'month\',a.date) = j.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Discount(INR)\') k on date_trunc(\'month\',a.date) = k.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'New Customers\') l on date_trunc(\'month\',a.date) = l.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Old Customers\') m on date_trunc(\'month\',a.date) = m.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Orders\') n on date_trunc(\'month\',a.date) = n.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'App Orders\') o on date_trunc(\'month\',a.date) = o.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Web Orders\') p on date_trunc(\'month\',a.date) = p.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'App Sessions\') q on date_trunc(\'month\',a.date) = q.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Web Sessions\') r on date_trunc(\'month\',a.date) = r.month ; Create or replace table Snitch_DB.MAPLEMONK.SALES_COST_CUSTOMERTYPE_SNITCH as with cost as ( select a.date, ifnull(a.cost,0)+ifnull(b.cost,0) as cost from (select date, sum(spend) as cost from Snitch_DB.MAPLEMONK.MARKETING_CONSOLIDATED_SNITCH group by 1 order by 1 desc) a left join ( select date,sum( case when lower(CHANNELS) = \'sms\' and date < \'2024-07-01\' then ifnull(replace(NOTIFICATION_SENT,\',\',\'\')::float,0)*0.12 when lower(CHANNELS) = \'sms\' and date >= \'2024-07-01\' then ifnull(replace(NOTIFICATION_SENT,\',\',\'\')::float,0)*0.10 when lower(CHANNELS) = \'whatsapp\' then ifnull(replace(NOTIFICATION_SENT,\',\',\'\')::float,0)*0.79 when lower(CHANNELS) = \'email\' then ifnull(replace(NOTIFICATION_SENT,\',\',\'\')::float,0)*0 when lower(CHANNELS) = \'push\' then ifnull(replace(NOTIFICATION_SENT,\',\',\'\')::float,0)*0 end) as cost from Snitch_DB.MAPLEMONK.Criteo_SMS_spends group by 1 order by 1 desc ) b on a.date=b.date ), month_level_sales as ( select date_trunc(\'month\',order_timestamp::date) as month ,count(distinct case when new_customer_flag = \'New\' then order_id end) as orders_all_new_prev_month ,count(distinct case when new_customer_flag = \'Repeated\' then order_id end) as orders_all_Repeated_prev_month ,count(distinct case when sku like \'%&%\' then order_id when right(sku,1) in (\'2\',\'3\',\'4\',\'5\',\'6\',\'7\',\'8\',\'9\') and new_customer_flag = \'New\' then order_id end ) as orders_combo_new_prev_month ,count(distinct case when sku like \'%&%\' then order_id when right(sku,1) in (\'2\',\'3\',\'4\',\'5\',\'6\',\'7\',\'8\',\'9\') and new_customer_flag = \'Repeated\' then order_id end ) as orders_combo_Repeated_prev_month ,sum(case when new_customer_flag = \'New\' then line_item_sales end) as gross_sales_new_prev_month ,sum(case when new_customer_flag = \'Repeated\' then line_item_sales end) as gross_sales_Repeated_prev_month ,sum(case when new_customer_flag = \'New\' then DISCOUNT end) as discount_new_prev_month ,sum(case when new_customer_flag = \'Repeated\' then DISCOUNT end) as discount_Repeated_prev_month ,sum(case when new_customer_flag = \'New\' then NET_SALES end) as net_sales_new_prev_month ,sum(case when new_customer_flag = \'Repeated\' then NET_SALES end) as net_sales_Repeated_prev_month from Snitch_DB.maplemonk.FACT_ITEMS_SNITCH a group by 1 ) select *, sum(net_sales_new)over(partition by year(order_date),month(order_date) order by order_date asc) as mtd_net_sales_new , sum(net_sales_Repeated)over(partition by year(order_date),month(order_date) order by order_date asc) as mtd_net_sales_Repeated , sum(orders_all_new)over(partition by year(order_date),month(order_date) order by order_date asc) as mtd_orders_all_new , sum(orders_all_Repeated)over(partition by year(order_date),month(order_date) order by order_date asc) as mtd_orders_all_Repeated , sum(discount_new)over(partition by year(order_date),month(order_date) order by order_date asc) as mtd_discount_new , sum(discount_Repeated)over(partition by year(order_date),month(order_date) order by order_date asc) as mtd_discount_Repeated , sum(cost)over(partition by year(order_date),month(order_date) order by order_date asc) as mtd_cost , sum(gross_sales_new)over(partition by year(order_date),month(order_date) order by order_date asc) as mtd_gross_sales_new , sum(gross_sales_Repeated)over(partition by year(order_date),month(order_date) order by order_date asc) as mtd_gross_sales_Repeated from ( select order_timestamp::date as order_date ,b.cost ,net_sales_new_prev_month ,net_sales_Repeated_prev_month ,discount_new_prev_month ,discount_Repeated_prev_month ,orders_all_new_prev_month ,orders_all_Repeated_prev_month ,count(distinct case when new_customer_flag = \'New\' then order_id end) as orders_all_new ,count(distinct case when new_customer_flag = \'Repeated\' then order_id end) as orders_all_Repeated ,count(distinct case when sku like \'%&%\' then order_id when right(sku,1) in (\'2\',\'3\',\'4\',\'5\',\'6\',\'7\',\'8\',\'9\') and new_customer_flag = \'New\' then order_id end ) as orders_combo_new ,count(distinct case when sku like \'%&%\' then order_id when right(sku,1) in (\'2\',\'3\',\'4\',\'5\',\'6\',\'7\',\'8\',\'9\') and new_customer_flag = \'Repeated\' then order_id end ) as orders_combo_Repeated ,sum(case when new_customer_flag = \'New\' then line_item_sales end) as gross_sales_new ,sum(case when new_customer_flag = \'Repeated\' then line_item_sales end) as gross_sales_Repeated ,sum(case when new_customer_flag = \'New\' then DISCOUNT end) as discount_new ,sum(case when new_customer_flag = \'Repeated\' then DISCOUNT end) as discount_Repeated ,sum(case when new_customer_flag = \'New\' then NET_SALES end) as net_sales_new ,sum(case when new_customer_flag = \'Repeated\' then NET_SALES end) as net_sales_Repeated from Snitch_DB.maplemonk.FACT_ITEMS_SNITCH a left join cost b on a.order_timestamp::date = b.date left join month_level_sales c on date_trunc(\'month\',add_months(a.order_timestamp::date, -1)) = month group by 1,2,3,4,5,6,7,8) order by 1 desc;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            