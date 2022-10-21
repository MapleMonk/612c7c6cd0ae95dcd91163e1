{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE table vahdam_db.maplemonk.D2C_Channelwise_sales as select order_timestamp::date as Order_date, \'Facebook\' as Channel, \'Fb Main\' as Account, \'Shopify\' as Source, sum(NET_SALES)-sum(tax) as Sales from vahdam_db.maplemonk.fact_items where lower(LANDING_UTM_CAMPAIGN) like \'%sc%\' and lower(shop_name) = \'shopify_usa\' and lower(final_utm_channel) like \'%facebook%\' group by order_timestamp::date union all select order_timestamp::date as Order_date, \'Facebook\' as Channel, \'Fb Whitelisting\' as Account, \'Shopify\' as Source, sum(NET_SALES)-sum(tax) as Sales from vahdam_db.maplemonk.fact_items where lower(LANDING_UTM_CAMPAIGN) like \'%prospecting%\' and lower(shop_name) = \'shopify_usa\' and lower(final_utm_channel) like \'%facebook%\' group by order_timestamp::date union all select order_timestamp::date as Order_date, \'Facebook\' as Channel, \'Fb Content\' as Account, \'Shopify\' as Source, sum(NET_SALES)-sum(tax) as Sales from vahdam_db.maplemonk.fact_items where lower(LANDING_UTM_CAMPAIGN) like \'%discovery%\' and lower(shop_name) = \'shopify_usa\' and lower(final_utm_channel) not like \'%google%\' group by order_timestamp::date union all select order_timestamp::date as Order_date, \'Google\' as Channel, \'Google\' as Account, \'Shopify\' as Source, sum(NET_SALES)-sum(tax) as Sales from vahdam_db.maplemonk.fact_items where lower(final_utm_channel) like \'%google%\' and lower(shop_name) = \'shopify_usa\' group by order_timestamp::date union all select date as Order_date, \'Facebook\' as Channel, \'Fb Main\' as Account, \'Platform\' as Source, sum(CONVERSION_VALUE) as Sales from vahdam_db.maplemonk.marketing_consolidated where channel =\'Facebook Ads\' and account = \'USA Main\' group by date union all select date as Order_date, \'Facebook\' as Channel, \'Fb Whitelisting\' as Account, \'Platform\' as Source, sum(CONVERSION_VALUE) as Sales from vahdam_db.maplemonk.marketing_consolidated where channel =\'Facebook Ads\' and account = \'Influencer USA\' group by date union all select date as Order_date, \'Facebook\' as Channel, \'Fb Content\' as Account, \'Platform\' as Source, sum(CONVERSION_VALUE) as Sales from vahdam_db.maplemonk.marketing_consolidated where channel =\'Facebook Ads\' and account = \'BMG US FB\' group by date union all select date as Order_date, \'Google\' as Channel, \'Google\' as Account, \'Platform\' as Source, sum(CONVERSION_VALUE) as Sales from vahdam_db.maplemonk.marketing_consolidated where channel =\'Google Ads\' group by date union all select order_timestamp::date as Order_date, \'Others_overall\' as Channel, \'Others_overall\' as Account, \'Shopify\' as Source, sum(NET_SALES)-sum(tax) as Sales from vahdam_db.maplemonk.fact_items where final_utm_channel = \'Others\' and lower(shop_name) = \'shopify_usa\' group by order_timestamp::date union all select order_timestamp::date as Order_date, \'Direct\' as Channel, \'Direct\' as Account, \'Shopify\' as Source, sum(NET_SALES)-sum(tax) as Sales from vahdam_db.maplemonk.fact_items where final_utm_channel = \'Direct\' and lower(shop_name) = \'shopify_usa\' group by order_timestamp::date union all select order_timestamp::date as Order_date, \'Instagram\' as Channel, \'Instagram\' as Account, \'Shopify\' as Source, sum(NET_SALES)-sum(tax) as Sales from vahdam_db.maplemonk.fact_items where final_utm_channel = \'Instagram\' and lower(shop_name) = \'shopify_usa\' group by order_timestamp::date union all select order_timestamp::date as Order_date, \'Email\' as Channel, \'Email\' as Account, \'Shopify\' as Source, sum(NET_SALES) as Sales from vahdam_db.maplemonk.fact_items where final_utm_channel = \'Email\' and lower(shop_name) = \'shopify_usa\' group by order_timestamp::date union all select order_timestamp::date as Order_date, \'SMS\' as Channel, \'SMS\' as Account, \'Shopify\' as Source, sum(NET_SALES)-sum(tax) as Sales from vahdam_db.maplemonk.fact_items where final_utm_channel = \'SMS\' and lower(shop_name) = \'shopify_usa\' group by order_timestamp::date union all select order_timestamp::date as Order_date, \'NULL\' as Channel, \'NULL\' as Account, \'Shopify\' as Source, sum(NET_SALES)-sum(tax) as Sales from vahdam_db.maplemonk.fact_items where final_utm_channel = \'null\' and lower(shop_name) = \'shopify_usa\' group by order_timestamp::date union all select order_timestamp::date as Order_date, \'Overall\' as Channel, \'Overall\' as Account, \'Shopify\' as Source, sum(NET_SALES)-sum(tax) as Sales from vahdam_db.maplemonk.fact_items where lower(shop_name) = \'shopify_usa\' group by order_timestamp::date union all select order_timestamp::date as Order_date, \'Others_actual\' as Channel, \'Others_actual\' as Account, \'Shopify\' as Source, case when sum(fi1.net_sales) = 0 then 0 else (ifnull(sum(fi1.net_sales),0)-ifnull(sum(fi1.tax),0))-(ifnull(fi2.Other_sales,0)-ifnull(fi2.Other_tax,0)) end as Sales from vahdam_db.maplemonk.fact_items fi1 left join (select order_timestamp::date as order_date, sum(net_sales) as Other_sales, sum(tax) as Other_tax from vahdam_db.maplemonk.fact_items where lower(shop_name) = \'shopify_usa\' and lower(landing_utm_campaign) like \'%discovery%\' group by order_timestamp::date order by order_timestamp::date) fi2 on fi1.order_timestamp::date = fi2.order_date where lower(final_utm_channel) = \'others\' and lower(shop_name) = \'shopify_usa\' group by order_timestamp::date, fi2.Other_sales, fi2.Other_tax order by Order_date desc",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from VAHDAM_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        