{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "select order_timestamp::date as Order_date, \'Facebook\' as Channel, \'Fb Main\' as Account, \'Shopify\' as Source, sum(NET_SALES) as Sales from vahdam_db.maplemonk.fact_items where lower(LANDING_UTM_CAMPAIGN) like \'%sc%\' and lower(shop_name) = \'shopify_usa\' and lower(final_utm_channel) like \'%facebook%\' group by order_timestamp::date union all select order_timestamp::date as Order_date, \'Facebook\' as Channel, \'Fb Whitelisting\' as Account, \'Shopify\' as Source, sum(NET_SALES) as Sales from vahdam_db.maplemonk.fact_items where lower(LANDING_UTM_CAMPAIGN) like \'%prospecting%\' and lower(shop_name) = \'shopify_usa\' and lower(final_utm_channel) like \'%facebook%\' group by order_timestamp::date union all select order_timestamp::date as Order_date, \'Facebook\' as Channel, \'Fb Content\' as Account, \'Shopify\' as Source, sum(NET_SALES) as Sales from vahdam_db.maplemonk.fact_items where lower(LANDING_UTM_CAMPAIGN) like \'%discovery%\' and lower(shop_name) = \'shopify_usa\' group by order_timestamp::date union all select order_timestamp::date as Order_date, \'Google\' as Channel, \'Google\' as Account, \'Shopify\' as Source, sum(NET_SALES) as Sales from vahdam_db.maplemonk.fact_items where lower(final_utm_channel) like \'%google%\' and lower(shop_name) = \'shopify_usa\' group by order_timestamp::date union all select date as Order_date, \'Facebook\' as Channel, \'Fb Main\' as Account, \'Platform\' as Source, sum(CONVERSION_VALUE) as Sales from vahdam_db.maplemonk.marketing_consolidated where channel =\'Facebook Ads\' and account = \'USA Main\' group by date union all select date as Order_date, \'Facebook\' as Channel, \'Fb Whitelisting\' as Account, \'Platform\' as Source, sum(CONVERSION_VALUE) as Sales from vahdam_db.maplemonk.marketing_consolidated where channel =\'Facebook Ads\' and account = \'Influencer USA\' group by date union all select date as Order_date, \'Facebook\' as Channel, \'Fb Content\' as Account, \'Platform\' as Source, sum(CONVERSION_VALUE) as Sales from vahdam_db.maplemonk.marketing_consolidated where channel =\'Facebook Ads\' and account = \'BMG US FB\' group by date union all select date as Order_date, \'Google\' as Channel, \'Google\' as Account, \'Platform\' as Source, sum(CONVERSION_VALUE) as Sales from vahdam_db.maplemonk.marketing_consolidated where channel =\'Google Ads\' group by date order by Order_date desc",
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
                        