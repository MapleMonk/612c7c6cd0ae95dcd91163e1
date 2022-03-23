{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table Gynoveda_DB.MAPLEMONK.Sales_Cost_CustomerType as with cost as ( select date, sum(spend) as cost from Gynoveda_DB.MAPLEMONK.MARKETING_CONSOLIDATED group by 1 order by 1 desc ) select order_timestamp::date as order_date , new_customer_flag ,b.cost ,count(distinct order_id) as orders_all ,count(distinct case when sku like \'%&%\' then order_id when right(sku,1) in (\'2\',\'3\',\'4\',\'5\',\'6\',\'7\',\'8\',\'9\') then order_id end ) as orders_combo ,sum(line_item_sales) as gross_sales ,sum(DISCOUNT) as discount ,sum(NET_SALES) as net_sales from Gynoveda_db.maplemonk.FACT_ITEMS a left join cost b on a.order_timestamp::date = b.date and a.new_customer_flag= \'New\' group by 1,2,3 order by 1 desc ; Create or replace table Gynoveda_DB.MAPLEMONK.Sales_Cost_ProductType as with cost2 as ( select date ,products product_flag ,sum(spend) as cost from Gynoveda_DB.MAPLEMONK.MARKETING_CONSOLIDATED left join Gynoveda_DB.MAPLEMONK.product_campaign_mapping p on p.product_links = rtrim(ltrim(ad_URL,\'[\"\'),\'\"]\') group by 1,2 ) select order_timestamp::date as order_date , new_customer_flag ,s.sku_name as product_flag ,b.cost ,count(distinct line_item_id) as orders ,sum(line_item_sales) as gross_sales ,sum(DISCOUNT) as discount ,sum(NET_SALES) as net_sales from Gynoveda_db.maplemonk.FACT_ITEMS a left join Gynoveda_DB.MAPLEMONK.skuname_mapping s on s.sku =a.sku left join cost2 b on a.order_timestamp::date = b.date and a.new_customer_flag= \'New\' and s.sku_name = b.product_flag group by 1,2,3,4 order by 1 desc ; Create or replace table Gynoveda_DB.MAPLEMONK.Sales_Cost_ReferringChannel as select order_timestamp::date as order_date ,a.referring_channel ,count(distinct order_id) as orders ,sum(line_item_sales) as gross_sales ,sum(DISCOUNT) as discount ,sum(NET_SALES) as net_sales ,avg(REPLACE( sessions , \',\' )) as sessions from Gynoveda_db.maplemonk.FACT_ITEMS a left join Gynoveda_db.maplemonk.shopify_sessions_data s on a.order_timestamp::date = to_date(s.date,\'DD-MM-YYYY\') and a.referring_channel = case when s.referring_channel = \'FB\' then \'Facebook\' when s.referring_channel = \'IG\' then \'Instagram\' when s.referring_channel = \'Search\' then \'Google\' else s.referring_channel end group by 1,2 ; Create or replace table Gynoveda_DB.MAPLEMONK.Cost_by_Channel as select date,channel, sum(spend) as cost from Gynoveda_DB.MAPLEMONK.MARKETING_CONSOLIDATED group by 1,2 ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GYNOVEDA_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        