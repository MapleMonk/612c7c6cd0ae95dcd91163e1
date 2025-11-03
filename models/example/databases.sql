{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.daily_inventory as select skucode sku ,FORMAT_TIMESTAMP(\'%Y-%m-%d\', a._airbyte_normalized_at) AS Date ,case when category like \'%SHIRT%\' or category like \'%JACKET%\' then \'Top\' when category like \'%SHORTS%\' or category like \'%TROUSER%\' then \'Bottom\' else \'Others\' end product_type ,facility ,inventory ,b.name ,category ,sub_Category ,size ,style_no ,image_link ,selling_price ,mrp from ( select * from ( select *, row_number() over (partition by skucode, facility order by _airbyte_normalized_At desc) rw from MAPLEMONK.Unicommerce_Unicommerce_get_inventory_snapshot ) where rw = 1 ) a left join ( select distinct SPLIT(product_code, \'_\')[OFFSET(0)] sku , name , image_url image_link , color style_no , upper(category_name) category , upper(description) sub_category , upper(size) size , mrp , base_price selling_price from maplemonk.banana_club_unicommerce_get_product_master where color <> \'\' qualify row_number() over(partition by lower(trim(SPLIT(product_code, \'_\')[OFFSET(0)])) order by 1 ) = 1 ) b on trim(lower(a.skucode)) = trim(lower(b.sku)); create or replace table Maplemonk.Bananaclub_inventory_ledger as SELECT FORMAT_DATE(\'%Y-%m-%d\', PARSE_DATE(\'%d-%b-%y\', Date)) AS Date, CAST(Brand AS STRING) AS Brand, CAST(SKU_Code AS STRING) AS SKU_Code, CAST(GRN_Count_Add_ AS INT64) AS GRN_Count_Add, CAST(Closing_Balance AS INT64) AS Closing_Balance, CAST(Opening_Balance AS INT64) AS Opening_Balance, CAST(Invoice_Count_Less_ AS INT64) AS Invoice_Count_Less, CAST(Inventory_Adjustment_Count AS INT64) AS Inventory_Adjustment_Count, CAST(Outbound_Gatepass_Count_Less_ AS INT64) AS Outbound_Gatepass_Count_Less, CAST(Outbound_Gatepass_Return_Count_Add_ AS INT64) AS Outbound_Gatepass_Return_Count_Add, CAST(Putaway_Cancelled_Return_Count_Add_ AS INT64) AS Putaway_Cancelled_Return_Count_Add, CAST(Inbound_Gatepass_Stock_Transfer_Add_ AS INT64) AS Inbound_Gatepass_Stock_Transfer_Add FROM MAPLEMONK.Bananaclub_get_inventory_ledger;",
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
            