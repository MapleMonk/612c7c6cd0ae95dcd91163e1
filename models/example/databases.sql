{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.sirona_marketplace_projections_fact_items AS WITH base AS ( SELECT a.reference_code, DATE(a.ORDER_Date) AS order_date, DATE(a.invoice_date) AS invoice_date, a.SKU, a.PRODUCTNAME AS product_name, a.order_type, CAST(a.number_of_products_in_combo AS INT64) AS item_quantity, CAST(a.suborder_quantity AS INT64) AS quantity, COALESCE(c.MP_Name,b.MP_Name) AS mp_name, c.channel, c.model_name FROM MAPLEMONK.sirona_wh_EasyEcom_FACT_ITEMS a LEFT JOIN ( SELECT DISTINCT Reference_Code, MP_Name FROM maplemonk.easyecom_new_tax_sales ) b ON a.reference_code = b.reference_code LEFT JOIN ( SELECT DISTINCT UPPER(mp_name) AS mp_name, UPPER(model_name) AS model_name, UPPER(channel) AS channel FROM maplemonk.googlesheet_marketplace_mapping ) c ON UPPER(b.mp_name) = UPPER(c.mp_name) WHERE DATE(a.ORDER_Date) >= DATE(\'2026-01-01\') ), week_number AS ( SELECT *, CEIL(EXTRACT(DAY FROM order_date) / 7.0) AS week_no FROM base ), week_pivot AS ( SELECT sku, product_name, order_date, week_no, item_quantity FROM week_number ), projection_map AS ( SELECT sku_code, SAP_CODE, Product_Name, Inventory_Holding_Code__EE_, Status__Offline, CAST(Projections AS INT64) AS projections FROM maplemonk.sirona_google_sheet_db_mp_planning ) SELECT a.sku, a.product_name, a.order_date, a.week_no, a.item_quantity, COALESCE(p1.projections , p2.projections) AS projections FROM week_pivot a LEFT JOIN projection_map p1 ON a.sku = p1.sap_code LEFT JOIN projection_map p2 ON a.sku = p2.sku_code",
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
            