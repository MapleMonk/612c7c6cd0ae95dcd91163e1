{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE andamen_db.maplemonk.andamen_db_sales_returns_Factitems AS SELECT City, State, \"AWB No\" AWB, Courier, \"MP Name\" Marketplace, \"Invoice Date\"::date Date, \"Order Status\", \"Payment Mode\" Payment_Mode, \"Component SKU\" as Component_SKU, \"Item Quantity\" as ItemQuantity, \"Sales Channel\" SalesChannel , \"Reference Code\" ReferenceCode, \"Component SKU Name\" SKUName, \"Component SKU Brand\" SKUbrand, \"Component SKU Category\" Category, case when lower(\"MP Name\") like \'%ajio%\' then \"Selling Price\"/0.664 else \"Selling Price\" end AS selling_Price, NULL AS ReturnAmount FROM andamen_db.maplemonk.andamen_db_tax_sales where ifnull(\"Invoice Date\",\'\') != \'\' UNION ALL SELECT City, State, \"AWB No\" AWB, Courier, \"MP Name\" Marketplace, nullif(\"Return Date\",\'\') ::date Date, \"Order Status\", \"Payment Mode\" Payment_Mode, \"Component SKU\" as Component_SKU, \"Item Quantity\" as ItemQuantity, \"Sales Channel\" SalesChannel , \"Reference Code\" ReferenceCode, \"Component SKU Name\" SKUName, \"Component SKU Brand\" SKUbrand, \"Component SKU Category\" Category, NULL AS sellingPrice, case when lower(\"MP Name\") like \'%ajio%\' then \"Selling Price\"/0.664 else \"Selling Price\" end AS ReturnAmount FROM andamen_db.maplemonk.andamen_db_tax_returns;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from andamen_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            