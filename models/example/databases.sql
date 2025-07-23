{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table trase-wh.maplemonk.trase_marketplace_sales as with Ad_spends as ( select date ,case when lower(channel) like any (\'%facebook%\', \'%google%\') then \'WEBSITE\' when lower(channel) like any (\'%amazon%\') then \'AMAZON\' when lower(channel) like \'%flipkart%\' then \'FLIPKART\' when lower(channel) like \'%myntra%\' then \'MYNTRA\' else upper(channel) end as Marketplace ,marketplace_id ,sum(ifnull(spend,0)) as spend ,sum(ifnull(impressions,0)) as ad_impressions ,sum(ifnull(clicks,0)) as ad_clicks ,sum(ifnull(conversions,0)) as ad_conversions ,sum(ifnull(conversion_value,0)) as ad_sales from trase-wh.maplemonk.TRASE_MARKETING_CONSOLIDATED group by 1,2,3 ), returns as ( select mapped_marketplace, order_id, order_date, reference_code, return_type, return_sku, marketplace_id, return_item, commonsku, color_sku, parent_sku, marketplace, return_flag, return_saleorderitemcode from trase-wh.maplemonk.TRASE_UNICOMMERCE_RETURNS where return_flag = 1 ), sessions as( Select cast(DATAENDTIME as date) Date, \'AMAZON\' as marketplace, parentAsin as marketplace_id, SUM(ifnull(cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.sessions\') as int64),0)) as sessions From `trase-wh.maplemonk.Amazon_Seller_partner_Business_Reports_Trase_Amazon_BR_GET_SALES_AND_TRAFFIC_REPORT_ASIN` ts group by 1,2,3 ), intermediate as ( select s.* ,r.return_type ,ifnull(r.return_flag,0) as final_return_flag ,m.spend spends ,m.ad_impressions impressions ,m.ad_clicks clicks ,m.ad_conversions conversions ,m.ad_sales ad_sale ,ses.sessions ,row_number() over (partition by coalesce(m.date,s.order_date),coalesce(m.marketplace_id,s.marketplace_id),upper(coalesce(m.marketplace,s.mapped_marketplace)) order by 1) rw from maplemonk.trase_sales_consolidated s left join Ad_spends m on s.order_date = m.date and s.marketplace_id = m.marketplace_id and lower(s.mapped_marketplace) = lower(m.marketplace) left join returns r on s.reference_code = r.reference_code and s.SALEORDERITEMCODE = r.return_saleorderitemcode left join sessions ses on ses.date = s.order_date and ses.marketplace_id = s.marketplace_id and lower(ses.marketplace) = lower(s.mapped_marketplace) ) select * ,case when rw = 1 then spends else 0 end as SPEND ,case when rw = 1 then impressions else 0 end as AD_IMPRESSIONS ,case when rw = 1 then CLICKS else 0 end as AD_CLICKS ,case when rw = 1 then conversions else 0 end as AD_CONVERSIONS ,case when rw = 1 then ad_sale else 0 end as AD_SALES from intermediate ;",
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
            