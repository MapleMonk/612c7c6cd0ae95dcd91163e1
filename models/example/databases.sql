{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.zouk_warehouse_TAT_Report AS SELECT Marketplace, Order_Date, u.reference_code, shippingPackageStatus, s.payment_mode, Dispatch_Date, invoiceDate, DATE_DIFF(dispatch_date,order_date,hour) AS Dispatch_TAT, DATE_DIFF(invoiceDate,order_date,hour) AS Packing_TAT, CASE WHEN DATE_DIFF(dispatch_date,order_date,hour) between 0 and 24 then \'0 - 24\' WHEN DATE_DIFF(dispatch_date,order_date,hour) between 25 and 48 then \'24 - 48\' WHEN DATE_DIFF(dispatch_date,order_date,hour) between 49 and 72 then \'48 - 72\' ELSE \'72 +\' END AS Dispatch_TimeRange, CASE WHEN DATE_DIFF(invoicedate,order_date,hour) between 0 and 24 then \'0 - 24\' WHEN DATE_DIFF(invoicedate,order_date,hour) between 25 and 48 then \'24 - 48\' WHEN DATE_DIFF(invoicedate,order_date,hour) between 49 and 72 then \'48 - 72\' ELSE \'72 +\' END AS Packing_TimeRange FROM `MapleMonk.zouk_UNICOMMERCE_FACT_ITEMS` u LEFT JOIN ( SELECT distinct LTRIM(ORDER_NAME,\'#\') as reference_code, payment_mode FROM `MapleMonk.zouk_SHOPIFY_FACT_ITEMS` where lower(marketplace) not like \'%pos%\' )s ON u.reference_code = s.reference_code where lower(marketplace) like \'shopify\' and lower(shippingPackageStatus) not like \'created\' ;",
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
            