{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table rubans_db.MAPLEMONK.rubans_PandL as select SALEORDERITEMCODE ,a.source as Marketing_Source ,a.channel as Marketing_Channel ,a.marketplace ,a.order_date as Date ,a.order_id ,a.reference_code ,a.shop_name ,a.return_flag ,a.new_customer_flag ,a.customer_id_final ,payment_mode ,payment_gateway ,a.awb ,a.order_Status ,a.shipping_status ,a.final_shipping_status ,case when lower(order_Status) like \'%cancel%\' then upper(order_status) else upper(coalesce(final_shipping_status,order_status)) end final_status ,a.sku ,a.sku_code ,d.brand ,a.product_name_final PRODUCT_NAME_FINAL ,a.product_sub_category product_sub_category ,a.product_category PRODUCT_CATEGORY ,a.quantity ,d.MRP*quantity MRP ,a.selling_price Gross_sale ,a.shipping_price shipping_price ,case when lower(coalesce(final_status,\'1\')) not in (\'cancelled\',\'rto\') then d.cogs*quantity else 0 end as COGS ,a.tax tax ,div0(ifnull(e.spend,0), count(1) over (partition by a.order_Date::date, a.channel)) as Paid_Marketing_Google ,div0(ifnull(f.spend,0), count(1) over (partition by a.order_Date::date, a.channel)) as Paid_Marketing_Facebook ,div0(ifnull(g.spend,0), count(1) over (partition by a.order_Date::date, a.channel)) as Paid_Marketing_Amazon ,case when a.new_customer_flag = \'Repeat\' then LAG(a.order_date) IGNORE NULLS OVER (partition by a.customer_id_final ORDER BY a.order_date) end previous_date ,datediff(day,previous_date,a.order_Date) days_from_last_order from rubans_db.MAPLEMONK.rubans_db_sales_consolidated a left join (select \"Product Code\" sku_code , brand ,\"TYPE\" ,avg(\"MRP\") mrp , coalesce(sum(\"Cost Price\"::float), sum(case when \"Component Price\" = \'\' then 0 else \"Component Price\"::float end)) Cogs from rubans_db.maplemonk.unicommerce_item_master group by 1,2,3 ) d on lower(replace(d.sku_code,\' \',\'\')) = lower(replace(a.sku_code,\' \',\'\')) left join (select date, sum(spend) spend from rubans_db.MAPLEMONK.rubans_db_MARKETING_CONSOLIDATED where lower(channel) like \'%google%\' group by date) e on e.date = a.order_Date::date and lower(case when lower(a.channel) like \'%google%\' then \'google\' end) like \'%google%\' left join (select date, sum(spend) spend from rubans_db.MAPLEMONK.rubans_db_MARKETING_CONSOLIDATED where lower(channel) like \'%facebook%\' group by date) f on f.date = a.order_Date::date and lower(case when lower(a.channel) like \'%facebook%\' then \'facebook\' end) like \'%facebook%\' left join (select date, sum(spend) spend from rubans_db.MAPLEMONK.rubans_db_MARKETING_CONSOLIDATED where lower(channel) like \'%amazon%\' group by date) g on g.date = a.order_Date::date and lower(a.channel) like \'%amazon%\' ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from rubans_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        