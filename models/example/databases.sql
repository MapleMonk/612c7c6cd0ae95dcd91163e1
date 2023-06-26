{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch_intermediate as with orders as ( select date(FI.order_timestamp::date) Date ,ifnull(sum(fi.net_sales),0) Total_Sales ,ifnull(sum(fi.gross_sales),0) Gross_Sales ,ifnull(sum(fi.Gross_sales),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.net_sales end),0) Gross_SALES_EXCL_CANCL ,count(distinct FI.order_id) Total_Orders ,count(distinct FI.order_id) - count(distinct case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.order_id end) Orders_EXCL_CANCL ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.order_id end)) - count(distinct(case when lower(FI.customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.order_id end)) as New_Customer_Orders_EXCL_CANCL ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.customer_id end)) as Total_New_Customers ,count(distinct(case when lower(FI.customer_flag) = \'new\' then FI.customer_id end)) - count(distinct(case when lower(FI.customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.customer_id end)) New_Customers_EXCL_CANCL ,count(distinct FI.customer_id) as TOTAL_Unique_Customers ,(count(distinct FI.customer_id) - count(distinct case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.customer_id end)) as Unique_Customers_EXCL_CANCL ,count(distinct(case when lower(FI.customer_flag) = \'repeat\' then FI.customer_id end)) as Repeat_Customers ,(count(distinct(case when lower(FI.customer_flag) = \'repeat\' then FI.customer_id end)) - count(distinct(case when lower(FI.customer_flag) = \'repeat\' and lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.customer_id end))) Repeat_Customers_EXCL_CANCL ,ifnull(sum(FI.discount),0) TOTAL_DISCOUNT ,(ifnull(sum(FI.discount),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.discount end),0)) TOTAL_DISCOUNT_EXCL_CANCL ,ifnull(sum(FI.tax),0) TOTAL_TAX ,(ifnull(sum(FI.tax),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.tax end),0)) TAX_EXCL_CANCL ,ifnull(sum(FI.shipping_price),0) TOTAL_SHIPPING_PRICE ,(ifnull(sum(FI.shipping_price),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.shipping_price end),0)) SHIPPING_PRICE_EXCL_CANCL ,ifnull(sum(case when lower(FI.customer_flag) = \'new\' then FI.discount end),0) as New_Customer_Discount ,(ifnull(sum(case when lower(FI.customer_flag) = \'new\' then FI.discount end),0) - ifnull(sum(case when lower(FI.customer_flag) = \'new\' and lower(order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.discount end),0)) as New_Customer_Discount_EXCL_CANCL ,ifnull(sum(FI.quantity),0) TOTAL_QUANTITY ,(ifnull(sum(FI.quantity),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') and FI.is_refund = 0 then FI.quantity end),0)) QUANTITY_EXCL_CANCL ,ifnull(sum(case when FI.is_refund=1 then FI.quantity end),0) as Return_Quantity ,ifnull(sum(case when FI.is_refund=1 then ifnull(FI.net_sales,0) end),0) as Return_Value ,count(distinct case when FI.is_refund=0 and lower(order_status) in (\'cancelled\') then order_id end) Cancelled_Orders ,count(distinct case when lower(order_status) not in (\'cancelled\') and is_refund=0 then order_id end) Net_Orders from Snitch_db.maplemonk.FACT_ITEMS_SNITCH FI group by 1 ), spend as (select date, sum(spend) as spend from Snitch_db.MAPLEMONK.MARKETING_CONSOLIDATED_SNITCH group by 1 ) select coalesce(fi.Date,MC.date) as date, Total_Sales, FI.Gross_Sales, Gross_SALES_EXCL_CANCL, Total_Orders, Orders_EXCL_CANCL, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, Return_Quantity, Return_Value, Cancelled_Orders, Net_Orders, spend as marketing_spend from orders FI full outer join spend MC on FI.Date = MC.date ; create or replace table snitch_db.maplemonk.sales_cost_source_mtd as select c.date, c.mtd_gross_sales, c.mtd_total_orders, c.mtd_total_new_customers, c.mtd_total_discount, c.mtd_marketing_spend, d.mtd_gross_sales lmtd_gross_sales, d.mtd_total_orders lmtd_total_orders, d.mtd_total_new_customers lmtd_total_new_customers, d.mtd_total_discount lmtd_total_discount, d.mtd_marketing_spend lmtd_marketing_spend from (select a.date, sum(b.gross_sales) mtd_gross_Sales, sum(b.total_orders) mtd_total_orders, sum(b.total_new_customers) mtd_total_new_customers, sum(b.total_discount) mtd_total_discount, sum(b.marketing_spend) mtd_marketing_spend from Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch_intermediate a left join (select date, gross_Sales, total_orders, total_new_customers, total_discount, marketing_spend from Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch_intermediate) b on a.date >= b.date and date_trunc(\'month\',a.date) = date_trunc(\'month\',b.date) group by 1) c left join (select a.date, sum(b.gross_sales) mtd_gross_Sales, sum(b.total_orders) mtd_total_orders, sum(b.total_new_customers) mtd_total_new_customers, sum(b.total_discount) mtd_total_discount, sum(b.marketing_spend) mtd_marketing_spend from Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch_intermediate a left join (select date, gross_Sales, total_orders, total_new_customers, total_discount, marketing_spend from Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch_intermediate) b on a.date >= b.date and date_trunc(\'month\',a.date) = date_trunc(\'month\',b.date) group by 1) d on d.date = dateadd(month,-1, c.date)::date ; create or replace table snitch_db.maplemonk.web_app_conversion_mtd as select c.date, c.mtd_web_Sessions, c.mtd_web_orders, c.mtd_appbrew_sessions, c.mtd_appbrew_orders, d.mtd_web_Sessions lmtd_web_Sessions, d.mtd_web_orders lmtd_web_orders, d.mtd_appbrew_sessions lmtd_appbrew_sessions, d.mtd_appbrew_orders lmtd_appbrew_orders from (select a.date, sum(b.web_sessions) mtd_web_Sessions, sum(b.web_orders) mtd_web_orders, sum(b.appbrew_Sessions) mtd_appbrew_sessions, sum(b.appbrew_orders) mtd_appbrew_orders from snitch_db.maplemonk.web_app_conversion_snitch a left join (select date, web_sessions, web_orders, appbrew_Sessions, appbrew_orders from snitch_db.maplemonk.web_app_conversion_snitch) b on a.date >= b.date and date_trunc(\'month\',a.date) = date_trunc(\'month\',b.date) group by 1 ) c left join (select a.date, sum(b.web_sessions) mtd_web_Sessions, sum(b.web_orders) mtd_web_orders, sum(b.appbrew_Sessions) mtd_appbrew_sessions, sum(b.appbrew_orders) mtd_appbrew_orders from snitch_db.maplemonk.web_app_conversion_snitch a left join (select date, web_sessions, web_orders, appbrew_Sessions, appbrew_orders from snitch_db.maplemonk.web_app_conversion_snitch) b on a.date >= b.date and date_trunc(\'month\',a.date) = date_trunc(\'month\',b.date) group by 1 ) d on d.date = dateadd(month,-1, c.date)::date ; Create or replace table Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch as select a.date as date, Total_Sales, A.GROSS_Sales, GROSS_SALES_EXCL_CANCL, a.Total_Orders, Orders_EXCL_CANCL, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, Return_Quantity, Return_Value, Cancelled_Orders, Net_Orders, marketing_spend, ifnull(b.customers,0) as Total_Customer_Till_Date, ifnull(b.gross_sales,0) as Sales_Till_Date, c.web_sessions, c.web_orders, c.appbrew_sessions, c.appbrew_orders, mtd_gross_sales, mtd_total_orders, mtd_total_new_customers, mtd_total_discount, mtd_marketing_spend, lmtd_gross_sales, lmtd_total_orders, lmtd_total_new_customers, lmtd_total_discount, lmtd_marketing_spend, mtd_web_Sessions, mtd_web_orders, mtd_appbrew_sessions, mtd_appbrew_orders, lmtd_web_Sessions, lmtd_web_orders, lmtd_appbrew_sessions, lmtd_appbrew_orders, f.target gross_Sales_target_month, g.target discount_target_month, h.target qty_per_order_target_month, i.target aov_target_month, j.target spends_target_month, k.target Discount_INR_target_month, l.target New_Customers_target_month, m.target Old_Customers_target_month, n.target Orders_target_month, o.target app_orders_target_month, p.target web_orders_target_month, q.target app_Sessions_target_month, r.target web_sessions_target_month, div0(right(a.date,2),right(last_day(a.date,\'month\'),2)) ratio, f.target::float*ratio gross_Sales_target_mtd, g.target::float discount_target_mtd, h.target::float qty_per_order_target_mtd, i.target::float aov_target_mtd, j.target::float*ratio spends_target_mtd, k.target::float*ratio Discount_INR_target_mtd, l.target::float*ratio New_Customers_target_mtd, m.target::float*ratio Old_Customers_target_mtd, n.target::float*ratio Orders_target_mtd, o.target::float*ratio app_orders_target_mtd, p.target::float*ratio web_orders_target_mtd, q.target::float*ratio app_Sessions_target_mtd, r.target::float*ratio web_sessions_target_mtd from Snitch_db.MAPLEMONK.Sales_Cost_Source_Snitch_intermediate a full outer join (select date ,sum(gross_sales) over (partition by 1 order by date asc rows between unbounded preceding and current row) gross_sales ,sum(customers) over (partition by 1 order by date asc rows between unbounded preceding and current row) customers from ( select A.order_timestamp::date date, ifnull(sum(a.Gross_sales),0) - ifnull(sum(case when lower(a.order_status) in (\'cancelled\') or a.is_refund = 1 then a.gross_sales end),0) gross_sales, count(distinct case when customer_flag = \'New\' then customer_id end) customers from Snitch_db.maplemonk.FACT_ITEMS_SNITCH A group by A.order_timestamp::date order by A.order_timestamp::date desc ) order by date desc ) b on a.Date = b.date left join snitch_db.maplemonk.web_app_conversion_snitch c on a.date = c.date left join snitch_db.maplemonk.sales_cost_source_mtd d on a.date = d.date left join snitch_db.maplemonk.web_app_conversion_mtd e on a.date = e.date left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Gross Sales\') f on date_trunc(\'month\',a.date) = f.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Discount\') g on date_trunc(\'month\',a.date) = g.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Qty Per Order\') h on date_trunc(\'month\',a.date) = h.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'AOV\') i on date_trunc(\'month\',a.date) = i.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Spends\') j on date_trunc(\'month\',a.date) = j.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Discount(INR)\') k on date_trunc(\'month\',a.date) = k.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'New Customers\') l on date_trunc(\'month\',a.date) = l.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Old Customers\') m on date_trunc(\'month\',a.date) = m.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Orders\') n on date_trunc(\'month\',a.date) = n.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'App Orders\') o on date_trunc(\'month\',a.date) = o.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Web Orders\') p on date_trunc(\'month\',a.date) = p.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'App Sessions\') q on date_trunc(\'month\',a.date) = q.month left join (select month, target from snitch_db.maplemonk.monthly_target where metric = \'Web Sessions\') r on date_trunc(\'month\',a.date) = r.month ;",
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
                        