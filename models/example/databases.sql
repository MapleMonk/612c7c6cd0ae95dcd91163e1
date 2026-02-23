{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.anveshan_executive_sales_summary; create table public.anveshan_executive_sales_summary as with sales as ( select marketplace, child_sub_category, date(order_date) as date, sum(coalesce(child_mrp::float8,0)) as mrp_sales, sum(coalesce(selling_price::float8,0)) as sp_sales, sum(coalesce(quantity,0)) as units from public.anveshan_sales_consolidated s group by 1,2,3 ), marketing_data AS ( SELECT date, CASE WHEN (LOWER(channel) LIKE \'%facebook%\' OR LOWER(channel) LIKE \'%google%\') THEN \'WEBSITE\' WHEN LOWER(channel) LIKE \'%amazon%\' THEN \'AMAZON\' WHEN LOWER(channel) LIKE \'%flipkart%\' THEN \'FLIPKART\' WHEN LOWER(channel) LIKE \'%myntra%\' THEN \'MYNTRA\' ELSE UPPER(channel) END AS Marketplace, category, SUM(COALESCE(spend, 0)) AS spend, SUM(COALESCE(impressions, 0)) AS ad_impressions, SUM(COALESCE(clicks, 0)) AS ad_clicks, SUM(COALESCE(conversions, 0)) AS ad_conversions, SUM(COALESCE(conversion_value, 0)) AS ad_sales FROM public.anweshan_MARKETING_CONSOLIDATED GROUP BY 1, 2, 3 ) select s.marketplace, s.child_sub_category, s.date, mrp_sales, sp_sales, units, m.ad_impressions, m.ad_conversions as ad_units, m.ad_conversions, m.ad_sales, m.spend as ad_spend from sales s left join marketing_data m on upper(m.category) = upper(s.child_sub_category) and m.date::date = date(s.date) and upper(s.marketplace) = upper(m.marketplace) ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            