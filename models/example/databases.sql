{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.ANVESHAN_AMAZON_VENDOR_PARTNER_FACT_ITEMS; CREATE TABLE public.ANVESHAN_AMAZON_VENDOR_PARTNER_FACT_ITEMS AS SELECT fi.asin::varchar AS Asin, startdate::date as StartTime, enddate::date as EndTime, orderedUnits::bigint AS OrderedUnits, CAST(orderedRevenue.amount AS double precision) AS OrderedRevenue, CAST(shippedRevenue.amount AS double precision) AS ShippedRevenue, CAST(shippedcogs.amount AS double precision) AS Shipped_Cogs, shippedunits::bigint AS ShippedUnits, customerreturns::bigint as Customer_Returns, p.commonsku as SKU_CODE, upper(coalesce(p.PRODUCT_name,a.product_name)) as PRODUCT_NAME_Final, Upper(coalesce(p.CATEGORY,a.category)) AS Product_Category, Upper(coalesce(a.sub_category)) AS Product_Sub_Category, Upper(coalesce(p.commonsku,a.commonsku)) AS commonsku, p.tax_rate AS pm_tax_rate, p.cogs AS cogs FROM public.Amazon_VP_GET_VENDOR_SALES_REPORT fi left join (select * from ( select \"(child) asin\"::varchar as asin, category::varchar as category, sku::varchar as commonsku, upper(replace(lower(title::varchar),\'anveshan \',\'\')) as product_name, \"sub category\"::varchar as sub_category, row_number() over (partition by \"(child) asin\"::varchar,sku order by priority::varchar) r from public.anveshan_amazon_asin_to_sku_mapping ) where r = 1 ) a on lower(replace(fi.asin,\' \',\'\')) = lower(replace(a.asin,\' \',\'\')) left join ( SELECT * FROM ( SELECT master_sku as commonsku, parent_category as category, tax_rate, product_name, parent_mrp, cogs, ROW_NUMBER() OVER (PARTITION BY master_sku ORDER BY LENGTH(COALESCE(master_sku, \'\')) DESC) rw FROM public.anveshan_sku_master ) WHERE rw = 1 ) p ON LOWER(REPLACE(REPLACE(a.commonsku::varchar, \' \', \'\'),\'\"\',\'\')) = LOWER(p.commonsku) ;",
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
            