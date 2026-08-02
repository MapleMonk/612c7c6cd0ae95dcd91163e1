{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.anveshan_bigbasket_ads_fact_items; CREATE TABLE public.anveshan_bigbasket_ads_fact_items AS select * FROM ( select cast(_airbyte_normalized_at as DATE) as date, cast(roas as double PRECISION) as ROAS, cast(CPM as double PRECISION) as CPM, cast(\"ad spend\" as double PRECISION) as spend, cast(\"ad revenue\" as double PRECISION) as revenue, cast(\"brand name\" as varchar) as brand, cast(\"product id\" as varchar) as product_id, cast(\"add to cart\" as int) as add_to_cart, cast(\"orders (sku)\" as int) as conversions, cast(\"product name\" as varchar) as product_name, cast(\"campaign name\" as varchar) as campaign_name, cast(\"ad impressions\" as int) as impressions, cast(\"other sku orders\" as int) as indirect_conversions, cast(\"other sku ad revenue\" as double PRECISION) as indirect_revenue, cast(\"same category orders\" as int) as same_category_conversions, category, row_number() over (partition by \"campaign name\",\"product id\",\"category\",\"product name\",date(_airbyte_normalized_at) order by _airbyte_normalized_at desc) as rw from public.bigbasket_anveshan_product_ads_report ) where rw = 1 ;",
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
            