{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Plaeto_wh_AMAZON_VENDOR_PARTNER AS SELECT COALESCE(CAST(fi.asin AS STRING),CAST(sr.asin AS STRING)) AS Asin, DATETIME(startTime, \'Asia/Kolkata\') AS StartTime, datetime(endtime,\"Asia/Kolkata\") AS EndTime, CAST(orderedUnits AS INT64) AS OrderedUnits, CAST(orderedRevenue AS float64) AS OrderedRevenue, sr.return_date, Customer_returns, SAFE_MULTIPLY( SAFE_DIVIDE(CAST(orderedRevenue AS FLOAT64), NULLIF(CAST(orderedUnits AS INT64), 0)),CAST(sr.Customer_returns AS INT64)) AS return_sales, p.skucode as SKU_CODE, upper(p.PRODUCT_name) as PRODUCT_NAME_Final, Upper(p.CATEGORY) AS Product_Category, Upper(p.category_gender) AS Product_Category_Gender, Upper(p.skucode) AS commonsku, Upper(p.colour_code) AS colour_code, Upper(p.product_code) AS product_code, Upper(p.style_name) AS style_name, Upper(p.sub_style_name) AS sub_style_name, upper(size) as size, upper(size_band) as size_band, upper(Colour) as Colour, upper(Colour_2) as Colour_2 FROM `MapleMonk.Amazon_VC_GET_VENDOR_REAL_TIME_SALES_REPORT` fi left join ( Select CAST(asin AS STRING) AS Asin, CAST(startDate as date) return_date, CAST(customerReturns as int64) as Customer_returns from `maplemonk.Plaeto_db_av_GET_VENDOR_SALES_REPORT` where customerReturns >0 qualify row_number() over (partition by asin, return_date order by 1)=1 ) sr on fi.Asin = sr.Asin and DATE(fi.StartTime ) = DATE(sr.return_date) left join (select * from ( select master_sku as skucode, marketplace_sku, colour_code, style_code, product_code, category_gender, style_name, sub_style_name, product_name, outsole_colour, category, size, size_band, Colour, Colour_2, fob, mrp, row_number() over (partition by lower(replace(marketplace_sku,\' \',\'\')) order by 1) rw from plaeup-wh.maplemonk.Final_SKU_master where lower(marketplace) like \'%amazon%\') where rw = 1 ) p on lower(replace(fi.asin,\' \',\'\')) = lower(replace(p.marketplace_sku,\' \',\'\')) ;",
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
            