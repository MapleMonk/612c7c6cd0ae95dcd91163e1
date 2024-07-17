{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table rpsg_db.maplemonk.drv_marketing_product_spends as WITH FACEBOOK AS ( select date, null as ad_type,ad_name,upper(channel) channel,product,category,spend from ( select date, ad_name, channel, ifnull(b.product,\'Others\') product, ifnull(b.category,\'Others\') category, row_number() over(partition by date,lower(ad_name),lower(channel) order by \"Ad Name Contains\" desc) rw, spend from ( select date, LOWER(coalesce(ad_name ,adset_name)) as ad_name, channel, sum(spend) spend from RPSG_DB.MAPLEMONK.MARKETING_CONSOLIDATED_DRV where date >= \'2023-04-01\' and lower(channel) = \'facebook\' group by 1,2,3 )a left join (select * from (select *, row_number() over (partition by \"Ad Name Contains\" order by 1) rw from rpsg_db.maplemonk.drv_facebook_ad_product_category ) where rw = 1 and \"Ad Name Contains\" is not null ) b on lower(a.ad_name) like \'%\' ||lower(b.\"Ad Name Contains\")|| \'%\' ) where rw= 1 ), GOOGLE_WITH_PERFORMANCE_MAX as ( select date, ad_type,adset_name,upper(channel) channel,upper(product)product,upper(category)category,spend from (select date, ad_type, adset_name, channel, ifnull(b.product,\'Others\') product, ifnull(b.category,\'Others\') category, row_number() over(partition by date, lower(ad_type),lower(adset_name),lower(title) order by lower(c1),lower(c2),lower(c3) desc ) rw, spend, from ( select \"segments.date\" as date, LOWEr(\"segments.product_custom_attribute0\") as adset_name, lower(\"segments.product_title\") as title, LOWER(\"campaign.advertising_channel_type\") as ad_type, \'GOOGLE\' as channel, sum(\"metrics.cost_micros\"/1000000) spend from (select * from rpsg_db.maplemonk.drv_new_test_product_data3 where lower(\"segments.product_custom_attribute0\" ) != \'combo\') group by 1,2,3,4 )a left join ( select campaign_type, replace(replace(replace(ifnull(ad_group_name_contains,\'\'),\'-\',\'\'),\' \',\'\'),\'_\',\'\') c1, replace(replace(replace(ifnull(additional_condition,\'\'),\'-\',\'\'),\' \',\'\'),\'_\',\'\') c2, replace(replace(replace(ifnull(negative_condition,\'\'),\'-\',\'\'),\' \',\'\'),\'_\',\'\') c3, negative_condition, product, category from ( select *, row_number() over (partition by lower(campaign_type),lower(ad_group_name_contains), lower(additional_condition),lower(negative_condition) order by 1) rw from rpsg_db.maplemonk.drv_google_ad_product_category where ad_group_name_contains is not null or additional_condition is not null or negative_condition is not null ) where rw = 1 ) b on lower(a.ad_type) = lower(b.campaign_type) and lower(replace(replace(replace(ifnull(a.adset_name,\'\'),\'-\',\'\'),\' \',\'\'),\'_\',\'\')) = lower(c1) and lower(replace(replace(replace(ifnull(a.title,\'\'),\'-\',\'\'),\' \',\'\'),\'_\',\'\')) like \'%\' || lower(c2) || \'%\' and not( case when b.negative_condition is null then false else lower(replace(replace(replace(ifnull(a.title,\'\'),\'-\',\'\'),\' \',\'\'),\'_\',\'\')) like \'%\'|| lower(c3) || \'%\' end ) ) where rw = 1 union all select date,\'performance_max\' as ad_type,null as adset_name,\'GOOGLE\' as channel,\'OTHERS\' as product,\'OTHERS\' as category,sum(spend) spend from ( select p.date ,p.campaign_id ,or_spend - p.spend as spend from ( select \"segments.date\" as date, \"campaign.id\" as campaign_id, sum(\"metrics.cost_micros\"/1000000) spend from (select * from rpsg_db.maplemonk.drv_new_test_product_data3) group by 1,2 )p left join ( select date ,campaign_id ,sum(spend) as or_spend from RPSG_DB.MAPLEMONK.MARKETING_CONSOLIDATED_DRV group by 1,2 )m on p.campaign_id = m.campaign_id and m.date = p.date ) group by 1 ), GOOGLE_WITHOUT_PERFORMANCE_MAX AS ( select date, ad_type,adset_name,upper(channel) channel,upper(product)product,upper(category)category,spend from (select date, ad_type, adset_name, channel, ifnull(b.product,\'Others\') product, ifnull(b.category,\'Others\') category, row_number() over(partition by date, lower(ad_type),lower(adset_name) order by lower(c1),lower(c2),lower(c3) desc ) rw, spend, from ( select date, LOWER(coalesce(adset_name,CAMPAIGN_NAME)) as adset_name, LOWER(ad_type)ad_type, channel, sum(spend) spend from ( select d.* from RPSG_DB.MAPLEMONK.MARKETING_CONSOLIDATED_DRV d left join ( select distinct \"segments.date\" as date,\"campaign.id\"as cid from rpsg_db.maplemonk.drv_new_test_product_data3 ) p on d.date = p.date and d.campaign_id = p.cid where (account_name = \'DRV Ad Account\' or account_name is null) and d.date >= \'2023-04-01\' and lower(channel) = \'google\' and cid is null ) group by 1,2,3,4 )a left join ( select campaign_type, replace(replace(replace(ifnull(ad_group_name_contains,\'\'),\'-\',\'\'),\' \',\'\'),\'_\',\'\') c1, replace(replace(replace(ifnull(additional_condition,\'\'),\'-\',\'\'),\' \',\'\'),\'_\',\'\') c2, replace(replace(replace(ifnull(negative_condition,\'\'),\'-\',\'\'),\' \',\'\'),\'_\',\'\') c3, negative_condition, product, category from ( select *, row_number() over (partition by lower(campaign_type),lower(ad_group_name_contains), lower(additional_condition),lower(negative_condition) order by 1) rw from rpsg_db.maplemonk.drv_google_ad_product_category where ad_group_name_contains is not null or additional_condition is not null or negative_condition is not null ) where rw = 1 ) b on lower(a.ad_type) = lower(b.campaign_type) and lower(replace(replace(replace(ifnull(a.adset_name,\'\'),\'-\',\'\'),\' \',\'\'),\'_\',\'\')) like \'%\' || lower(c1) || \'%\' || lower(c2) || \'%\' and not( case when b.negative_condition is null then false else lower(replace(replace(replace(ifnull(a.adset_name,\'\'),\'-\',\'\'),\' \',\'\'),\'_\',\'\')) like \'%\'|| lower(c3) || \'%\' end ) ) where rw = 1 ), RETENTION AS ( select p_s.date as date ,NULL AS ad_type ,NULL as adset_name ,\'RETENTION\' AS CHANNEL ,product ,category ,(r.spend)*IFNULL(share,0) as product_spend from ( select * ,div0(items_send,sum(items_send) over(partition by date)) share from ( select try_to_date(date,\'MM/DD/YYYY\') date ,upper(product) as product ,upper(category) as category ,sum(replace(count,\',\',\'\')::int) items_send from rpsg_db.maplemonk.drv_retention where tags != \'P2\' group by 1,2,3 ) ) p_s left join ( select try_to_date(date, \'yyyy-mm-dd\') pre_date ,sum(try_cast(replace(spend,\',\',\'\') as float)) spend from rpsg_db.maplemonk.retention_spend group by 1 ) r on r.pre_date = (p_s.date) ), CRITEO as ( select day::date date ,NULL AS ad_type ,NULL as adset_name ,\'CRITEO\' AS CHANNEL ,\'OTHERS\' AS product ,\'OTHERS\' AS category ,sum(advertisercost) as spend from rpsg_db.maplemonk.criteo_campaign_statistics group by 1 ), google_combo as ( select pt.date ,NULL AS ad_type ,NULL as adset_name ,\'GOOGLE\' AS CHANNEL ,coalesce(product,\'OTHERS\') AS PRODUCT ,COALESCE(category,\'OTHERS\') AS CATEGORY ,spend*IFNULL((replace(\"Product contribution\",\'%\',\'\')::int /100),1) AS SPEND FROM ( select \"segments.date\" as date ,\"segments.product_item_id\" ,sum(\"metrics.cost_micros\"/1000000) spend from rpsg_db.maplemonk.drv_new_test_product_data3 where lower(\"segments.product_custom_attribute0\" ) = \'combo\' group by 1,2 )pt left join ( select * from (select \"Product ID (contains)\" as pid ,\"Product \" as product ,\"Category \" as Category ,\"Product contribution\" ,row_number() over(partition by \"Custom Label 0\",\"Product ID (contains)\",\"Product \",\"Category \" order by 1)rw from rpsg_db.maplemonk.drv_google_ad_combos_conditions )where rw=1 )map on LOWER(pt.\"segments.product_item_id\") like \'%\' || LOWER(map.pid) || \'%\' ) SELECT * FROM FACEBOOK UNION ALL SELECT * FROM GOOGLE_WITH_PERFORMANCE_MAX where date < \'2024-04-01\' or date >= \'2024-07-01\' UNION ALL SELECT * FROM GOOGLE_WITHOUT_PERFORMANCE_MAX where date < \'2024-04-01\' or date >= \'2024-07-01\' UNION ALL SELECT * FROM RETENTION UNION ALL SELECT * FROM CRITEO UNION ALL SELECT * FROM google_combo where date < \'2024-04-01\' or date >= \'2024-07-01\' union all ( select try_to_date(date,\'MM/DD/YYYY\') date ,NULL AS ad_type ,NULL as adset_name ,\'GOOGLE\' AS CHANNEL ,\"PRODUCT NAME\" product ,\"PRODUCT CATEGORY\" Category ,sum(replace(spends,\',\',\'\')) spends from rpsg_db.maplemonk.drv_dod_updated_spends_google_apr_may_june group by 1,2,3,4,5,6 ) ;",
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
                        