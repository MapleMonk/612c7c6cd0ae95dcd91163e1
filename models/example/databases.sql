{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table izf-wh.maplemonk.izf_knot_fact_items as select SKU, cast(coalesce(size,split(replace(UPPER(trim(parts[SAFE_OFFSET(ARRAY_LENGTH(parts) - 1)])),\'_\',\'\'),\'/Z\')[SAFE_OFFSET(0)]) as string) as sku_size, replace(replace(parts[safe_offset(0)],\'`\',\'\'),\'\\'\',\'\') sku_style, cast(FORMAT_DATE(\'%Y-%m-%d\',PARSE_DATE(\'%d-%b-%y\', Sold_on))as date) Order_Date, cast(quantity as int64) as Quantity, Hex_Color, Product_URL, Color_Family as Colour, Product_Name, cast(selling_Price as float64) as Selling_Price from (select *,ARRAY( SELECT part FROM UNNEST(SPLIT(replace(sku,\'_\',\'-\'), \'-\')) AS part WHERE part != \'\' ) as parts from maplemonk.izf_knot_sales ) ;",
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
            