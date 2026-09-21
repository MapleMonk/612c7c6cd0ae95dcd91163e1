{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.anveshan_bigbasket_ads_fact_items; CREATE TABLE public.anveshan_bigbasket_ads_fact_items AS select * FROM ( select cast(date as DATE) as date, start_date, end_date, cast(\"campaign name\" as varchar) as campaign_name, cast(roas as double PRECISION) as ROAS, cast(CPM as double PRECISION) as CPM, cast(\"ad spend\" as double PRECISION) as spend, cast(\"ad revenue\" as double PRECISION) as revenue, cast(\"brand name\" as varchar) as brand, cast(\"product id\" as varchar) as product_id, cast(\"add to cart\" as int) as add_to_cart, cast(\"orders (sku)\" as int) as conversions, cast(\"product name\" as varchar) as product_name, cast(\"ad impressions\" as int) as impressions, cast(\"other sku orders\" as int) as indirect_conversions, cast(\"other sku ad revenue\" as double PRECISION) as indirect_revenue, cast(\"same category orders\" as int) as same_category_conversions, category, row_number() over (partition by date,cast(\"campaign name\" as varchar),cast(\"product id\" as varchar),cast(\"product name\" as varchar),category order by start_date desc,end_date desc, date desc) as rw from public.bigbasket_ads_anveshan_product_ads_report ) where rw = 1 ; drop table if exists public.bigbasket_ads_keyword_report_fact_items; create table public.bigbasket_ads_keyword_report_fact_items as select cast(left(date,10) as date) as Date, upper(trim(keyword)) as keyword, cast(\"ad spend\" as DOUBLE PRECISION) as ad_spend, cast(\"ad revenue\" as DOUBLE PRECISION) as ad_revenue, upper(trim(\"match type\")) as keyword_match_type, \"campaign id\" as campaign_id, \"keyword id\" as keyword_id, upper(trim(\"campaign name\")) as campaign_name, cast(\"ad impressions\" as int4) as impressions, cast(cpm as double precision) as cpm, cast(ctr as double precision) as ctr, cast(roas as double precision) as roas from public.bigbasket_ads_anveshan_keyword_ads_report ;",
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
            