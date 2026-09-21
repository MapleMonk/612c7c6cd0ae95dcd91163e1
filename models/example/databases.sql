{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.ANVESHAN_AMAZON_VENDOR_PARTNER_FACT_ITEMS; CREATE TABLE public.ANVESHAN_AMAZON_VENDOR_PARTNER_FACT_ITEMS AS SELECT replace(fi.asin::varchar,\'\"\',\'\') AS Asin, startdate::date as StartTime, enddate::date as EndTime, orderedUnits::bigint AS OrderedUnits, CAST(orderedRevenue.amount AS double precision) AS OrderedRevenue, CAST(shippedRevenue.amount AS double precision) * 1.05 AS ShippedRevenue, CAST(shippedcogs.amount AS double precision) AS Shipped_Cogs, shippedunits::bigint AS ShippedUnits, customerreturns::bigint as Customer_Returns, p.commonsku as SKU_CODE, upper(coalesce(p.PRODUCT_name)) as PRODUCT_NAME_Final, Upper(coalesce(p.CATEGORY)) AS Product_Category, null AS Product_Sub_Category, Upper(coalesce(p.commonsku)) AS commonsku, p.tax_rate AS pm_tax_rate, p.cogs AS cogs FROM public.Amazon_VP_GET_VENDOR_SALES_REPORT fi LEFT JOIN ( SELECT * FROM ( SELECT master_sku as commonsku, amazon_asin as marketplace_sku, parent_category as category, tax_rate, product_name, parent_mrp, cogs, ROW_NUMBER() OVER (PARTITION BY amazon_asin ORDER BY LENGTH(COALESCE(amazon_asin, \'\')) DESC) rw FROM public.anveshan_sku_master ) WHERE rw = 1 ) p ON trim(LOWER(REPLACE(REPLACE(fi.asin::varchar, \' \', \'\'),\'\"\',\'\'))) = trim(LOWER(p.marketplace_sku)) ;",
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
            