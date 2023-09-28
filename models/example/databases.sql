{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table rpsg_db.MAPLEMONK.DIGITAL_MIS_DRV AS with traffic_cte as ( select distinct ga_date as date ,m.mapped_marketplace mapped_marketplace ,x.shop_name as marketplace ,channel ,sum(case when lower(channel) in (\'direct\',\'organic\') then (ifnull(ga_users,0)) end) as traffic ,sum(case when lower(channel) not in (\'direct\',\'organic\') then (ifnull(ga_users,0)) end) as paid_traffic from rpsg_db.maplemonk.GA_Sessions_Consolidated_DRV X left join (select * from ( select * , row_number() over (partition by lower(marketplace) order by 1) rw from rpsg_db.maplemonk.drv_marketplace_mapping ) where rw=1 ) m on lower(x.shop_name) = lower(m.marketplace) group by 1,2,3,4 order by 1 desc ) ,invoicedatemetrics as ( select try_to_date(X.invoice_date) Date ,concat(monthname(try_to_date(X.invoice_date)),\'-\',year(try_to_date(X.invoice_date))) month ,concat(\'Week\',\'-\',week(try_to_date(X.invoice_date))) week ,X.GA_CHANNEL Channel ,m.mapped_marketplace Mapped_Marketplace ,x.marketplace as Marketplace ,sum(ifnull((case when lower(order_status) not in (\'cancelled\') then ifnull(X.SELLING_PRICE,0)-ifnull(X.TAX,0) end),0)) Realised_Revenue from RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_DRV X left join (select * from (select * ,row_number() over (partition by lower(marketplace) order by 1) rw from rpsg_db.maplemonk.drv_marketplace_mapping ) where rw=1 ) m on lower(x.marketplace) = lower(m.marketplace) group by 1,2,3,4,5,6 ) ,returnsales as ( select return_date::date date ,concat(monthname(return_date::date),\'-\',year(return_date)) month ,concat(\'Week\',\'-\',week(return_date::date)) week ,channel ,m.mapped_marketplace as MAPPED_MARKETPLACE ,x.marketplace as marketplace ,sum(total_return_amount) TOTAL_RETURN_AMOUNT ,sum(total_return_amount_excl_tax) TOTAL_RETURN_AMOUNT_EXCL_TAX ,sum(total_returned_quantity) TOTAL_RETURNED_QUANTITY from RPSG_DB.MAPLEMONK.easyecom_returns_summary_drv x left join (select * from (select * , row_number() over (partition by lower(marketplace) order by 1) rw from rpsg_db.maplemonk.drv_marketplace_mapping ) where rw=1 ) m on lower(x.marketplace) = lower(m.marketplace) group by 1,2,3,4,5,6 order by 1 desc ) ,cte as ( select distinct Date ,channel ,month ,week ,x.marketplace as marketplace ,m.mapped_marketplace as MAPPED_MARKETPLACE ,sum(do_orders) as DO_orders ,sum(booked_orders) as booked_orders ,sum(Booked_Orders_Ex_Cancl) as Booked_Orders_Ex_Cancl ,sum(booked_revenue) as booked_revenue ,sum(Booked_Revenue_Ex_Cancl) as Booked_Revenue_Ex_Cancl ,sum(Booked_Revenue_new) as Booked_Revenue_New ,sum(Booked_Revenue_repeat) as Booked_Revenue_Repeat ,sum(discount) as discount ,sum(tax) as tax ,sum(return_sales) as return_sales ,sum(ifnull(Booked_Revenue_Ex_Cancl,0) - ifnull(tax,0)) as Booked_Revenue_Ex_Canclandtax from ( select order_date::date as Date ,concat(monthname(order_date::date),\'-\',year(order_Date)) month ,concat(\'Week\',\'-\',week(order_date::date)) week ,marketplace ,ga_channel channel ,new_customer_flag ,count(distinct order_id) as booked_orders ,count(distinct case when lower(ga_channel) in (\'direct\',\'organic\') and lower(order_status) <> \'cancelled\' then order_id end) do_orders ,count(distinct case when lower(order_status) <> \'cancelled\' then order_id end) as Booked_Orders_Ex_Cancl ,sum(selling_price) as booked_revenue ,sum(mrp_sales) as mrp_sales ,sum(selling_price) as booked_sales ,sum(ifnull(selling_price,0) - ifnull(return_sales,0) - ifnull((case when lower(order_status) = \'cancelled\' then selling_price end),0)) as Booked_Revenue_Ex_Cancl ,case when lower(new_customer_flag) = \'new\' then sum(ifnull(selling_price,0)) end as Booked_Revenue_new ,case when lower(new_customer_flag) <> \'new\' then sum(ifnull(selling_price,0)) end as Booked_Revenue_Repeat ,sum(discount) as discount ,sum(tax) as tax ,sum(return_sales) as return_sales from sales_consolidated_drv group by 1,2,3,4,5,6 order by 1 desc ) x left join (select * from (select * ,row_number() over (partition by lower(marketplace) order by 1) rw from rpsg_db.maplemonk.drv_marketplace_mapping ) where rw=1 ) m on lower(x.marketplace) = lower(m.marketplace) group by 1,2,3,4,5,6 order by 1 desc ) select coalesce(a.Date,b.date, c.date, RS.date,ID.date) Date ,coalesce(a.channel, b.channel, RS.Channel,ID.Channel) Channel ,coalesce(a.month, b.month,c.month,RS.month,ID.month) Month ,coalesce(a.week, b.week,c.week,RS.week,ID.week) Week ,coalesce(a.marketplace,b.marketplace,c.marketplace, Rs.marketplace,ID.marketplace) as marketplace ,coalesce(a.mapped_marketplace,b.mapped_marketplace, c.mapped_marketplace, RS.mapped_marketplace,ID.mapped_marketplace) as MAPPED_MARKETPLACE ,ifnull(a.DO_orders,0) as DO_orders ,ifnull(a.booked_orders,0) as booked_orders ,ifnull(a.Booked_Orders_Ex_Cancl,0) as Booked_Orders_Ex_Cancl ,ifnull(a.booked_revenue,0) as booked_revenue ,ifnull(a.Booked_Revenue_Ex_Cancl,0) as Booked_Revenue_Ex_Cancl ,ifnull(a.Booked_Revenue_new,0) as Booked_Revenue_new ,ifnull(a.Booked_Revenue_repeat,0) as Booked_Revenue_repeat ,ifnull(a.discount,0) as discount ,ifnull(a.tax,0) as tax ,ifnull(a.return_sales,0) as return_sales ,ifnull(a.Booked_Revenue_Ex_Canclandtax,0) as Booked_Revenue_Ex_Canclandtax ,ifnull(b.spend,0) as Spend ,RS.TOTAL_RETURN_AMOUNT_EXCL_TAX ,RS.TOTAL_RETURNED_QUANTITY ,ID.Realised_Revenue ,c.traffic/count(1) over(partition by coalesce(a.Date,b.date,c.date),coalesce(a.marketplace,b.marketplace, c.marketplace) order by coalesce(a.Date,b.date,c.date) desc) as traffic ,c.paid_traffic/count(1) over(partition by coalesce(a.Date,b.date,c.date),coalesce(a.marketplace,b.marketplace,c.marketplace) order by coalesce(a.Date,b.date,c.date) desc) as paid_traffic from cte a full outer join (select X.* ,m.mapped_marketplace Mapped_Marketplace from (select date ,channel ,case when lower(account) like (\'%vaidya%\') then \'Shopify_DRV\' when lower(account) like (\'%herbobuild%\') then \'Shopify_Herbobuild\' when lower(account) like (\'%ayurvedic%\') then \'Shopify_AyurvedicSource\' end marketplace ,concat(monthname(date),\'-\',year(Date)) month ,concat(\'Week\',\'-\',week(date)) as Week ,sum(clicks) as clicks ,sum(spend) as spend ,sum(conversions) as conversions from rpsg_db.maplemonk.marketing_consolidated_drv x group by 1,2,3,4 order by 1 desc ) x left join (select * from (select * ,row_number() over (partition by lower(marketplace) order by 1) rw from rpsg_db.maplemonk.drv_marketplace_mapping ) where rw=1 ) m on lower(x.marketplace) = lower(m.marketplace) ) b on lower(a.channel) = lower(b.channel) and a.date = b.date and lower(a.marketplace) = lower(b.marketplace) full outer join ( select date ,concat(monthname(date),\'-\',year(Date)) month ,concat(\'Week\',\'-\',week(date)) as Week ,mapped_marketplace ,marketplace ,sum(ifnull(traffic,0)) as traffic ,sum(ifnull(paid_traffic,0)) as paid_traffic from traffic_cte group by 1,2,3,4,5 ) c on (case when lower(coalesce(a.mapped_marketplace,b.mapped_marketplace)) = \'website\' then coalesce(a.date,b.date) end) = c.date and (case when lower(coalesce(a.mapped_marketplace,b.mapped_marketplace)) = \'website\' then lower(coalesce(a.marketplace,b.marketplace)) end) = lower(c.marketplace) full outer join returnsales RS on RS.date = coalesce(a.date,b.date,c.date) and lower(RS.Channel) = lower(coalesce(a.Channel,b.channel)) and lower(RS.marketplace)=lower(coalesce(a.marketplace,b.marketplace,c.marketplace)) full outer join invoicedatemetrics ID on ID.date = coalesce(a.date,b.date,c.date,RS.date) and lower(ID.Channel) = lower(coalesce(a.Channel,b.channel,RS.channel)) and lower(ID.marketplace)=lower(coalesce(a.marketplace,b.marketplace,c.marketplace,RS.marketplace)) order by 1 desc ; create or replace table rpsg_db.maplemonk.Product_performance_report_drv as with sales_cte as ( select distinct order_date::date date ,sku ,sku_type ,product_id ,productname ,marketplace ,channel ,new_customer_flag ,count(distinct order_id) as orders ,count(distinct customer_id_final,customer_name,state) as customers ,sum(selling_price) as gross_sales ,sum(ifnull(selling_price,0) - ifnull(return_sales,0) - ifnull(cancel_sales,0)) as net_sales ,sum(discount) as discount ,sum(case when lower(sku_type) = \'combo\' then selling_price end) as Combo_sales ,sum(case when lower(new_customer_flag) = \'new\' then selling_price end) First_time_order_sales ,sum(case when lower(new_customer_flag) <> \'new\' then selling_price end) Repeat_sales from sales_consolidated_drv group by 1,2,3,4,5,6,7,8 ) select c.Date ,m.mapped_marketplace as MARKETPLACE ,m.segments as Segment ,c.channel ,Product_Category_Mapped as Product ,c.product_id ,c.productname ,sum(c.orders) as orders ,sum(c.customers) as customers ,sum(c.gross_sales) as gross_sales ,sum(c.net_sales) as net_sales ,sum(c.discount) as discount ,sum(c.combo_Sales) as combo_sales ,sum(c.First_time_order_sales) as First_time_order_sales ,sum(c.Repeat_sales) as Repeat_sales ,sum(a.sales)::number as lm_sales ,sum(ma.sales)::number as llm_sales from sales_cte c left join drv_marketplace_mapping m on lower(c.marketplace) = lower(m.marketplace) left join ( select date_trunc(month,date) date,sku,sku_type,product_id,marketplace,channel, sum(gross_sales) sales from sales_cte group by 1,2,3,4,5,6) a on dateadd(month,-1,date_trunc(month,c.date)) = a.date and c.product_id =a.product_id and c.marketplace = a.marketplace and c.channel =a.channel left join (select date_trunc(month,date) date ,sku,sku_type,product_id,marketplace,channel, sum(gross_sales) as sales,sum(orders) as orders from sales_cte group by 1,2,3,4,5,6 ) ma on dateadd(month,-2,date_trunc(month,c.date)) = ma.date and c.product_id =ma.product_id and c.marketplace = ma.marketplace and c.channel =ma.channel left join (select * from (select distinct sku, sku_type, productname, \"DRV Category\" Product_Category_Mapped, row_number() over (partition by sku, sku_type, productname order by \"DRV Category\") rw from rpsg_DB.maplemonk.sku_master where \"DRV Category\" is not null) where rw=1 ) S on lower(c.sku)=lower(s.sku) and lower(c.sku_type)=lower(s.Sku_type) and lower(c.productname)= lower(s.productname) group by 1,2,3,4,5,6,7 order by 1 desc; create or replace table rpsg_db.maplemonk.ga_data_table_drv as with cte as ( select distinct order_date::date as date,b.channel from sales_consolidated_drv cross join (select distinct ga_channel as channel from sales_consolidated_drv) b ), sessions as ( select ga_date as date , channel, case when lower(channel) in (\'direct\',\'organic\') then \'Free\' else \'Paid\' end as category ,sum(ga_users) users,sum(ga_sessions) sessions,sum(case when lower(channel) in (\'direct\',\'organic\') then ga_sessions end) free_sessions from rpsg_db.maplemonk.ga_sessions_consolidated_drv group by 1,2 order by 1 desc ), channel_revenue as( select order_date::date as date, ga_channel channel,count(distinct case when lower(order_status) not in (\'cancelled\',\'returned\') then order_id end) as orders, sum(selling_price) - sum(ifnull(cancel_sales,0)) - sum(ifnull(return_sales,0)) as revenue from sales_consolidated_drv group by 1,2 order by 1 desc ,2 asc ) select a.date ,a.channel ,case when lower(a.channel) in (\'direct\',\'organic\') then \'Free\' else \'Paid\' end as category ,orders ,revenue ,t.users ,t.sessions ,coalesce(ws.spend,m.spend) as spend from cte a left join channel_revenue cr on a.date =cr.date and lower(a.channel) =lower(cr.channel) left join sessions t on a.date=t.date and lower(a.channel) = lower(t.channel) left join (select date,channel,sum(spend) as spend from marketing_consolidated_drv group by 1,2)m on a.date= m.date and lower(a.channel) = lower(m.channel) left join (select try_to_date(date, \'yyyy-mm-dd\') date ,upper(channel) CHANNEL ,sum(try_cast(replace(spend,\',\',\'\') as float)) spend from rpsg_db.maplemonk.drv_website_channel_marketing_spend group by 1,2 ) ws on a.date =ws.date and lower(a.channel) = lower(ws.channel); create or replace table rpsg_db.maplemonk.ga_data_table_drv as select x.*,sum(x.revenue) over (partition by x.date) as total_revenue from GA_DATA_TABLE_DRV x;",
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
                        