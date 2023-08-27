{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table mymuse_db.maplemonk.mymuse_db_GA_produt_funnel_shopify as select a.* ,ifnull(b.orders,0) + ifnull(c.orders,0) + ifnull(d.orders,0) + ifnull(e.orders,0) orders ,ifnull(b.items_purchased,0) + ifnull(c.items_purchased,0) + ifnull(d.items_purchased,0) + ifnull(e.items_purchased,0) items_purchased from (select to_date(date, \'yyyymmdd\') Date ,PROPERTY_ID ,ITEMID ,ITEMNAME ,sum(ifnull(SESSIONS,0)) SESSIONS ,sum(ifnull(ENGAGEDSESSIONS,0)) ENGAGEDSESSIONS ,sum(ifnull(ITEMSCHECKEDOUT,0)) ITEMSCHECKEDOUT ,sum(ifnull(ITEMSADDEDTOCART,0)) ITEMSADDEDTOCART from mymuse_db.maplemonk.GOOGLE_ANALYTICS_4__GA4__GA4_MYMUSE_PRODUCTS_FUNNEL_METRICS group by 1,2,3,4 ) a left join ( select sku , order_timestamp::Date order_Date , count(distinct ordeR_id) orders , sum(quantity) items_purchased from MYMUSE_DB.MAPLEMONK.MYMUSE_DB_SHOPIFY_FACT_ITEMS group by 1,2 ) b on a.date = b.order_Date and lower(a.itemid) = lower(b.sku) left join ( select concat(\'shopify_IN\',product_id,\'_\',variant_id) GA_product_id ,order_timestamp::Date order_Date ,count(distinct order_id) orders , sum(quantity) items_purchased from MYMUSE_DB.MAPLEMONK.MYMUSE_DB_SHOPIFY_FACT_ITEMS FI group by 1,2 ) c on a.date = c.order_Date and lower(a.itemid) = lower(c.GA_product_id) left join ( select product_id GA_product_id ,order_timestamp::Date order_Date ,count(distinct order_id) orders , sum(quantity) items_purchased from MYMUSE_DB.MAPLEMONK.MYMUSE_DB_SHOPIFY_FACT_ITEMS FI group by 1,2 ) d on a.date = d.order_Date and lower(a.itemid) = lower(d.GA_product_id) left join ( select variant_id GA_product_id ,order_timestamp::Date order_Date ,count(distinct order_id) orders , sum(quantity) items_purchased from MYMUSE_DB.MAPLEMONK.MYMUSE_DB_SHOPIFY_FACT_ITEMS FI group by 1,2 ) e on a.date = e.order_Date and lower(a.itemid) = lower(e.GA_product_id) ; create or replace table mymuse_db.maplemonk.mymuse_db_GA_landing_page_funnel_shopify as select * from MYMUSE_DB.MAPLEMONK.google_analytics_4__ga4__ga4_mymuse_landing_page_funnel_metrics; create or replace table mymuse_db.maplemonk.mymuse_db_GA_marketing_channel_funnel_shopify as select * from MYMUSE_DB.MAPLEMONK.google_analytics_4__ga4__ga4_mymuse_marketing_channel_funnel_metrics;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MYMUSE_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        