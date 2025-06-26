{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table trase-wh.maplemonk.trase_marketplace_sales as with Ad_spends as ( select date ,case when lower(channel) like any (\'%facebook%\', \'%google%\') then \'WEBSITE\' when lower(channel) like any (\'%amazon%\') then \'AMAZON\' when lower(channel) like \'%flipkart%\' then \'FLIPKART\' when lower(channel) like \'%myntra%\' then \'MYNTRA\' else upper(channel) end as Marketplace ,marketplace_id ,sum(ifnull(spend,0)) as spend ,sum(ifnull(impressions,0)) as ad_impressions ,sum(ifnull(clicks,0)) as ad_clicks ,sum(ifnull(conversions,0)) as ad_conversions ,sum(ifnull(conversion_value,0)) as ad_sales from trase-wh.maplemonk.TRASE_MARKETING_CONSOLIDATED group by 1,2,3 ), returns as ( select mapped_marketplace, order_id, order_date, reference_code, return_type, return_sku, marketplace_id, return_item, commonsku, color_sku, parent_sku, marketplace, return_flag, return_saleorderitemcode from trase-wh.maplemonk.TRASE_UNICOMMERCE_RETURNS where return_flag = 1 ) select s.* ,r.return_type ,ifnull(r.return_flag,0) as final_return_flag ,m.spend ,m.ad_impressions ,m.ad_clicks ,m.ad_conversions ,m.ad_sales from maplemonk.trase_sales_consolidated s left join Ad_spends m on s.order_date = m.date and s.marketplace_id = m.marketplace_id and lower(s.mapped_marketplace) = lower(m.marketplace) left join returns r on s.reference_code = r.reference_code and s.SALEORDERITEMCODE = r.return_saleorderitemcode;",
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
            