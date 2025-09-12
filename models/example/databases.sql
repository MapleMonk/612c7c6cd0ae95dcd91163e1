{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table trase-wh.maplemonk.trase_meesho_ads_consolidated as select \'MEESHO\' as ACCOUNT_NAME ,NULL as ACCOUNT_ID ,cast(null as string) as Campaign_Name ,cast(null as string) as Campaign_ID ,cast(FORMAT_DATE(\'%Y-%m-%d\', PARSE_DATE(\'%d/%m/%y\', date)) as date) as Date ,cast(FORMAT_DATE(\'%A\', PARSE_DATE(\'%d/%m/%y\',date))as string) as DAY_OF_WEEK ,EXTRACT(YEAR FROM cast(PARSE_DATE(\'%d/%m/%y\', date)as date)) AS year ,EXTRACT(MONTH FROM cast(PARSE_DATE(\'%d/%m/%y\',date)as date)) AS month ,\'MEESHO\' as Channel ,\'MEESHO\' as Account ,cast(replace(catalog_id,\',\',\'\') as string) as Catalog_id ,sm.new_sku as sku ,sum(cast(replace(ad_clicks,\',\',\'\') as int64)) clicks ,sum(cast(replace(ad_spend,\',\',\'\') as float64)) spends ,sum(cast(replace(ad_views,\',\',\'\') as int64)) impressions ,sum(cast(replace(ad_orders,\',\',\'\') as int64)) conversions ,sum(cast(replace(ad_gmv,\',\',\'\') as float64)) conversion_value from maplemonk.S3_Bucket_meesho_ads m left join (select new_sku,marketplace_id from trase-wh.maplemonk.final_sku_master where lower(marketplace) like \'%meesho%\' qualify row_number() over(partition by trim(marketplace_id) order by 1) = 1 ) sm on replace(cast(sm.marketplace_id as string),\' \',\'\') = replace(cast(replace(m.catalog_id,\',\',\'\') as string),\' \',\'\') group by 1,2,3,4,5,6,7,8,9,10,11,12;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            