{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE andamen_db.maplemonk.andamen_db_sales_returns_Factitems AS SELECT City, State, \"AWB No\" AWB, Courier, \"MP Name\" Marketplace, \"Invoice Date\"::date Date, \"Order Status\", \"Payment Mode\" Payment_Mode, \"Component SKU\" as Component_SKU, \"Item Quantity\" as ItemQuantity, \"Sales Channel\" SalesChannel , \"Reference Code\" ReferenceCode, \"Component SKU Name\" SKUName, \"Component SKU Brand\" SKUbrand, \"Component SKU Category\" Category, DISCOUNT_CODE, case when not(lower(ifnull(DISCOUNT_CODE,\'\')) in (\'moc\',\'mog\',\'challan\')) then 1 else 0 end as Discount_Flag, case when lower(\"MP Name\") like \'%ajio%\' then \"Selling Price\"/0.664 else \"Selling Price\" end AS selling_Price, NULL AS ReturnAmount, \"Component SKU MRP\" as MRP FROM andamen_db.maplemonk.andamen_db_tax_sales ts left join ( SELECT name AS Order_Name, upper(B.VALUE:code::String) AS DISCOUNT_CODE FROM andamen_db.maplemonk.shopify_andamen_website_orders,LATERAL FLATTEN (INPUT => DISCOUNT_CODES)B qualify row_number() over(partition by lower(trim(name)) order by 1) = 1 )ds on trim(lower(ts.\"Reference Code\")) = trim(lower(ds.Order_Name)) where ifnull(\"Invoice Date\",\'\') != \'\' UNION ALL SELECT City, State, \"AWB No\" AWB, Courier, \"MP Name\" Marketplace, nullif(\"Return Date\",\'\') ::date Date, \"Order Status\", \"Payment Mode\" Payment_Mode, \"Component SKU\" as Component_SKU, \"Item Quantity\" as ItemQuantity, \"Sales Channel\" SalesChannel , \"Reference Code\" ReferenceCode, \"Component SKU Name\" SKUName, \"Component SKU Brand\" SKUbrand, \"Component SKU Category\" Category, DISCOUNT_CODE, case when not(lower(ifnull(DISCOUNT_CODE,\'\')) in (\'moc\',\'mog\',\'challan\')) then 1 else 0 end as Discount_Flag, NULL AS sellingPrice, case when lower(\"MP Name\") like \'%ajio%\' then \"Selling Price\"/0.664 else \"Selling Price\" end AS ReturnAmount, \"Component SKU MRP\" as MRP FROM andamen_db.maplemonk.andamen_db_tax_returns ts left join ( SELECT name AS Order_Name, upper(B.VALUE:code::String) AS DISCOUNT_CODE FROM andamen_db.maplemonk.shopify_andamen_website_orders,LATERAL FLATTEN (INPUT => DISCOUNT_CODES)B qualify row_number() over(partition by lower(trim(name)) order by 1) = 1 )ds on trim(lower(ts.\"Reference Code\")) = trim(lower(ds.Order_Name)) ;",
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
            