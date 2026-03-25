{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.Sirona_fill_rate_fact_items as select a.reference_code, date(a.ORDER_Date) Order_Date, a.sku, PRODUCTNAME Product_Name, suborder_quantity Quantity, a.order_type, a.order_Status, CASE WHEN UPPER(order_status) = \'RETURNED\' THEN suborder_quantity ELSE 0 END AS returned_Qty, CASE WHEN UPPER(order_status) = \'OPEN\' THEN suborder_quantity ELSE 0 END AS open_Qty, CASE WHEN UPPER(order_status) = \'CANCELLED\' THEN suborder_quantity ELSE 0 END AS cancelled_qty, CASE WHEN UPPER(order_status) = \'SHIPPED\' THEN suborder_quantity ELSE 0 END AS shipped_qty, CASE WHEN UPPER(order_status) = \'CONFIRMED\' THEN suborder_quantity ELSE 0 END AS confirmed_qty, coalesce(c.MP_Name,b.MP_Name) MP_Name, c.channel, c.model_name, d.Current_Inventory from MAPLEMONK.sirona_wh_EasyEcom_FACT_ITEMS a left join( select Distinct Reference_Code, MP_Name from maplemonk.easyecom_new_tax_sales) b on a.reference_code = b.reference_code left join (select distinct upper(mp_name) mp_name, upper(model_name) model_name, upper(channel) channel from maplemonk.googlesheet_marketplace_mapping) c on upper(b.mp_name) = upper(c.mp_name) left join( select sku, sum(CAST(Quantity as int64)) Current_Inventory, FROM `Maplemonk.Sirona_inv_db_consolidated_inventory` WHERE DATE(_airbyte_emitted_at) = CURRENT_DATE() AND Company_Name =\'SIRONA HYGIENE PRIVATE LIMITED(Tauru)\' group by 1) d ON TRIM(UPPER(a.SKU)) = TRIM(UPPER(d.SKU)) where upper(Warehouse_name) = \'SIRONA HYGIENE PRIVATE LIMITED(TAURU)\' and a.order_type not in(\'B2C\') and date(a.ORDER_Date) >=\'2026-02-01\';",
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
            