{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.ANVESHAN_AMAZON_VENDOR_PARTNER_FACT_ITEMS; CREATE TABLE public.ANVESHAN_AMAZON_VENDOR_PARTNER_FACT_ITEMS AS SELECT asin::varchar AS Asin, startdate::date as StartTime, enddate::date as EndTime, orderedUnits::bigint AS OrderedUnits, CAST(orderedRevenue.amount AS double precision) AS OrderedRevenue, CAST(shippedcogs.amount AS double precision) AS Shipped_Cogs, shippedunits::bigint AS ShippedUnits, customerreturns::bigint as Customer_Returns, p.commonsku as SKU_CODE, upper(p.PRODUCT_name) as PRODUCT_NAME_Final, Upper(p.CATEGORY) AS Product_Category, Upper(p.sub_category) AS Product_Sub_Category, Upper(p.commonsku) AS commonsku FROM public.Amazon_VP_GET_VENDOR_SALES_REPORT fi LEFT JOIN ( SELECT * FROM ( SELECT ean, commonsku, category, sub_category, gst_rate, product_name, new_hsn_from_17_sept_25, product_launch_date, product_image_url, cogs, ROW_NUMBER() OVER (PARTITION BY commonsku ORDER BY LENGTH(COALESCE(commonsku, \'\')) DESC) rw FROM public.anweshan_sku_master ) WHERE rw = 1 ) p on lower(replace(fi.asin,\' \',\'\')) = lower(replace(p.commonsku,\' \',\'\')) ;",
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
            