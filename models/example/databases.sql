{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table hilodesign_db.MAPLEMONK.Sales_Cost_Source_hilo_web as with orders as ( select date(FI.order_date) Date ,FI.SOURCE Channel ,ifnull(sum(case when lower(order_status) not in (\'cancelled\') then FI.selling_price end),0) Gross_Sales ,ifnull(sum(case when lower(order_status) not in (\'cancelled\') and payment_gateway = \'COD\' then FI.selling_price end),0) COD_Gross_Sales ,ifnull(sum(case when lower(order_status) not in (\'cancelled\') and payment_gateway = \'Prepaid\' then FI.selling_price end),0) Prepaid_Gross_Sales ,count(distinct case when lower(order_status) not in (\'cancelled\') then FI.order_id end) Orders ,count(distinct case when lower(order_status) not in (\'cancelled\') and payment_gateway = \'COD\' then FI.order_id end) COD_Orders ,count(distinct case when lower(order_status) not in (\'cancelled\') and payment_gateway = \'Prepaid\' then FI.order_id end) Prepaid_Orders ,count(distinct case when lower(order_status) not in (\'cancelled\') and payment_gateway = \'Exchange\' then FI.order_id end) Exchange_Orders ,count(distinct case when lower(order_status) not in (\'cancelled\') and payment_gateway = \'Test\' then FI.order_id end) Test_Orders ,count(distinct(case when lower(order_status) not in (\'cancelled\') and lower(FI.new_customer_flag) = 1 then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(order_status) not in (\'cancelled\') and lower(FI.new_customer_flag) = 1 then FI.customer_id end)) as New_Customers ,count(distinct case when lower(order_status) not in (\'cancelled\') then FI.customer_id_final end) as Unique_Customers ,count(distinct(case when lower(FI.new_customer_flag) = 0 and lower(order_status) not in (\'cancelled\') then FI.customer_id_final end)) as Repeat_Customers ,ifnull(sum(case when lower(order_status) not in (\'cancelled\') then FI.suborder_quantity end),0) Gross_QUANTITY ,ifnull(sum(case when is_refund=1 and lower(order_status) not in (\'cancelled\') then FI.suborder_quantity end),0) as Return_Quantity ,ifnull(sum(case when is_refund=1 and lower(order_status) not in (\'cancelled\') then ifnull(FI.selling_price,0) end),0) as Return_Value ,ifnull(sum(case when lower(order_status) not in (\'cancelled\') then FI.cogs::float end),0) as COGS ,Gross_Sales - Return_Value as Net_Sales from hilodesign_db.maplemonk.SALES_CONSOLIDATED_HILO FI where lower(FI.marketplace) in (\'shopify_india\') group by 1,2 order by date desc ) , spend as (select date,channel, sum(spend) as spend from hilodesign_db.MAPLEMONK.MARKETING_CONSOLIDATED_HILO group by 1,2 ) select coalesce(fi.Date,MC.date) as date, coalesce(FI.Channel, MC.channel) as channel, Gross_Sales, COD_Gross_sales, Prepaid_Gross_sales, Orders, COD_orders, Prepaid_orders, Exchange_orders, Test_Orders, New_Customer_Orders, New_Customers, Unique_Customers, Repeat_Customers, Gross_QUANTITY, Return_Quantity, Return_Value, COGS, spend as marketing_spend, Net_Sales from orders FI full outer join spend MC on FI.Date = MC.date and FI.Channel = MC.channel ; Create or replace table hilodesign_db.MAPLEMONK.Sales_Cost_Source_hilo as with targets as ( select c.date, c.sales_channel, t.target/count(distinct c.date) over (partition by c.sales_channel, date_trunc(\'month\',c.date)) target from (select a.date, b.sales_channel from (select my_date date from hilodesign_db.maplemonk.DATE_DIMENSION where my_date > \'2019-01-01\') a cross join (select distinct sales_channel sales_channel from hilodesign_db.maplemonk.sales_cost_source_hilo) b ) c left join (select date, target, channel from hilodesign_db.maplemonk.target) t on date_trunc(\'month\',c.date) = date_trunc(\'month\',t.date::date) and c.sales_channel = case when t.channel = \'Assist\' then \'POS\' when t.channel = \'Shopiy\' then \'Online\' else t.channel end ) , sales as ( select marketplace,sales_channel ,order_date::date as ORDER_DATE ,count(distinct case when new_customer_flag = 1 and lower(order_status) <> \'cancelled\' and payment_gateway not in (\'Exchange\',\'Test\') then order_id end) as orders_new ,coalesce(sum(case when new_customer_flag = 1 and lower(order_status) <> \'cancelled\' and payment_gateway not in (\'Exchange\',\'Test\') then selling_price end),0) as gross_sales_new ,sum(case when new_customer_flag = 1 and lower(order_status) <> \'cancelled\' and payment_gateway not in (\'Exchange\',\'Test\') then suborder_quantity end ) as total_quantity_new ,sum(case when new_customer_flag = 1 then returned_quantity end ) as returned_quantity_new ,sum(case when new_customer_flag = 1 then cancelled_quantity end ) as cancelled_quantity_new ,sum(case when new_customer_flag = 1 then return_sales end ) as return_sales_new ,sum(case when new_customer_flag = 1 then cancel_sales end ) as cancel_sales_new ,ifnull(gross_sales_new,0) - ifnull(return_sales_new,0) as net_sales_new ,sum(case when lower(order_status) <> \'cancelled\' and payment_gateway not in (\'Exchange\',\'Test\') and is_refund <> \'1\' then ifnull(tax,0) end) tax ,count(distinct case when new_customer_flag = 0 and lower(order_status) <> \'cancelled\' and payment_gateway not in (\'Exchange\',\'Test\') then order_id end) as orders_repeat ,coalesce(sum(case when new_customer_flag = 0 and lower(order_status) <> \'cancelled\' and payment_gateway not in (\'Exchange\',\'Test\') then selling_price end),0) as gross_sales_repeat ,sum(case when new_customer_flag = 0 and lower(order_status) <> \'cancelled\' and payment_gateway not in (\'Exchange\',\'Test\') then suborder_quantity end ) as total_quantity_repeat ,sum(case when new_customer_flag = 0 then returned_quantity end ) as returned_quantity_repeat ,sum(case when new_customer_flag = 0 then cancelled_quantity end ) as cancelled_quantity_repeat ,sum(case when new_customer_flag = 0 then return_sales end ) as return_sales_repeat ,sum(case when new_customer_flag = 0 then cancel_sales end ) as cancel_sales_repeat ,ifnull(gross_sales_repeat,0) - ifnull(return_sales_repeat,0) as net_sales_repeat ,count(distinct case when lower(order_status) in (\'cancelled\') then order_id end) as orders_cancelled ,count(distinct case when lower(order_status) not in (\'cancelled\') and payment_gateway not in (\'Exchange\',\'Test\') then customer_id_final end) as Unique_Customers ,count(distinct case when new_customer_flag = 1 and lower(order_status) not in (\'cancelled\') and payment_gateway not in (\'Exchange\',\'Test\') then customer_id_final end) as New_Customers ,ifnull(sum(case when lower(order_status) <> \'cancelled\' and payment_gateway not in (\'Exchange\',\'Test\') then cogs::float end),0) as COGS ,sum(ifnull(return_cogs,0)) return_cogs from hilodesign_db.maplemonk.sales_consolidated_hilo group by 1,2,3 ), marketing as ( select date, case when objective = \'OUTCOME_LEADS\' then \'POS\' when account in (\'Facebook India\',\'Google India\') and objective is null or objective <> \'OUTCOME_LEADS\' then \'Online\' end as sales_channel ,sum(spend) as spend, sum(conversions) orders,sum(conversion_value) Sales , sum(case when objective = \'OUTCOME_LEADS\' then spend end) as Style_Assist_FB_spend , sum(case when account = \'Facebook India\' and objective <> \'OUTCOME_LEADS\' then spend end) as FB_spend , sum(case when account = \'Google India\' then spend end) as Google_spend from hilodesign_db.MAPLEMONK.MARKETING_CONSOLIDATED_HILO a group by 1,2 ) select s.marketplace as marketplace ,coalesce(t.sales_channel,s.sales_channel) as sales_channel ,coalesce(t.date,s.order_date,m.date) as order_date ,t.target ,sum(t.target) over (partition by t.sales_channel,date_trunc(\'month\',t.Date)) monthly_target ,orders_new ,gross_sales_new ,sum(gross_sales_new) over (partition by t.sales_channel, date_trunc(\'month\',t.date)) mtd_gross_Sales_new ,total_quantity_new ,returned_quantity_new ,cancelled_quantity_new ,return_sales_new ,cancel_sales_new ,net_sales_new ,orders_repeat ,gross_sales_repeat ,sum(gross_sales_repeat) over (partition by t.sales_channel, date_trunc(\'month\',t.date)) mtd_gross_Sales_repeat ,total_quantity_repeat ,returned_quantity_repeat ,cancelled_quantity_repeat ,return_sales_repeat ,cancel_sales_repeat ,net_sales_repeat ,orders_cancelled ,Unique_Customers ,New_Customers ,cogs ,sum(cogs) over (partition by t.sales_channel, date_trunc(\'month\',t.date)) mtd_cogs ,return_cogs ,tax ,m.spend as spend ,m.Style_Assist_FB_spend ,m.FB_spend ,m.Google_spend ,m.sales as ads_sales ,m.orders as ads_orders from targets t full join sales s on t.date = s.order_Date::date and t.sales_channel = s.sales_channel full join marketing m on t.date::date = m.date::date and m.sales_channel = t.sales_channel order by t.date::date desc;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HILODESIGN_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        