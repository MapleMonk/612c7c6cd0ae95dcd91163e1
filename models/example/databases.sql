{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.HEALTHY_MASTER_AMAZON_NOW_FACT_ITEMS AS SELECT cast(fi.asin as string) AS Asin, date(cast(startdate as datetime)) as StartTime, date(cast(enddate as datetime)) as EndTime, cast(orderedUnits as int64) AS OrderedUnits, CAST(JSON_EXTRACT_SCALAR(orderedRevenue,\'$.amount\') AS float64) AS OrderedRevenue, CAST(JSON_EXTRACT_SCALAR(shippedRevenue,\'$.amount\') AS float64) AS ShippedRevenue, CAST(JSON_EXTRACT_SCALAR(shippedCogs,\'$.amount\') AS float64) AS Shipped_Cogs, CAST(shippedunits as int64) AS ShippedUnits, CAST(customerreturns as int64) as Customer_Returns, upper(coalesce(p.name)) AS product_name_final, COALESCE(UPPER(cast(p.CATEGORY as string))) AS product_category, cast(null as string) AS product_sub_category, upper(coalesce(p.product_type)) product_type, upper(coalesce(p.master_sku)) commonsku FROM maplemonk.Amazon_Now_GET_VENDOR_SALES_REPORT fi left join (select Product_title name, Product_Type, Category, Market_place_product_ID sku_code, master_sku, Market_place_products_name from maplemonk.google_sheets_sku_master where lower(platform) like \'%amazon%\' qualify row_number() over (partition by lower(replace(Market_place_product_ID,\' \',\'\')) order by 1)=1 ) p on lower(replace(fi.asin,\' \',\'\')) = lower(replace(p.sku_code,\' \',\'\')) ;",
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
            