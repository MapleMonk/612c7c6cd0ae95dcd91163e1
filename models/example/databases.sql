{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table rubans_db.maplemonk.rubans_myntra_vendor_flex_Fact_Items as select * ,case when position(\'%\',GST,0) > 0 then try_cast(replace(GST,\'%\',\'\') as float)/100 else try_cast(GST as float) end GST_Final from (select upper(\'Myntra_Vendor_Flex\') as marketplace ,try_to_date(INVOICE,\'dd-MON-yy\') Invoice_Date ,SKU ,CONCAT(try_to_date(INVOICE,\'dd-MON-yy\'),\'-\',SKU) as Order_ID ,CONCAT(try_to_date(INVOICE,\'dd-MON-yy\'),\'-\',SKU) as Order_LINE_ID ,Upper(brand) BRAND ,\"GST %\" GST ,try_to_decimal(\"Qty.\") Quantity ,upper(CATEGORY) CATEGORY ,try_cast(MRP as float) MRP ,try_cast(\"Invoice Value\" as float) Invoice_Value ,try_Cast(\"Selling Price\" as float) Selling_Price ,try_cast(MRP as float) - try_Cast(\"Selling Price\" as float) MRP_Discount ,try_cast(\"Invoice Value\" as float) - try_Cast(\"Selling Price\" as float) Invoice_Discount ,row_number() over (partition by INVOICE, SKU order by _AIRBYTE_NORMALIZED_AT desc) rw from rubans_db.maplemonk.rubans_myntra_vendor_flex_orders ) where rw=1;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from rubans_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        