{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table Franchise_Model as SELECT combined.*, CASE WHEN combined.retailer_name IN (SELECT DISTINCT code FROM crm_overview) THEN \'Franchise\' ELSE NULL END AS franchise_flag FROM ( SELECT Date, NULL AS first_month, retailer_name, area_classification, sale_type, SKU, PRODUCT_TYPE, RETAILER_TYPE, DISTRIBUTOR, NULL AS DEALER_NAME, REVENUE, eggs_sold FROM primary_and_secondary_sku UNION ALL SELECT DELIVERY_DATE AS Date, NULL AS first_month, code AS retailer_name, area_classification, CASE WHEN LOWER(retailer_category) = \'tertiary\' THEN \'tertiary\' END AS sale_type, sku, product_type, retailer_category AS RETAILER_TYPE, distributor_name AS DISTRIBUTOR, DEALER_NAME, sku_sale AS REVENUE, eggs_sold FROM tertiary_sales UNION ALL SELECT crm_date AS Date, first_month, code AS retailer_name, NULL AS area_classification, CASE WHEN LOWER(code) IS NOT NULL THEN \'Franchise\' END AS sale_type, crm_sku AS SKU, NULL AS product_type, NULL AS RETAILER_TYPE, NULL AS DISTRIBUTOR, NULL AS DEALER_NAME, crm_sale AS REVENUE, crm_eggs_sold AS eggs_sold FROM crm_overview ) AS combined;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EGGOZDB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            