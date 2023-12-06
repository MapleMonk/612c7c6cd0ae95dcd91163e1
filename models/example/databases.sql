{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "REVERSE(SUBSTRING(REVERSE(\"Vendor Article Number\"), CHARINDEX(\'-\', REVERSE(\"Vendor Article Number\")) + 1)) select distinct REVERSE(SUBSTRING(REVERSE(\"Vendor Article Number\"), CHARINDEX(\'-\', REVERSE(\"Vendor Article Number\")) + 1)) from Elcinco_db.maplemonk.myntra_sjit_sales ; create or replace table Elcinco_db.maplemonk.Elcinco_Myntra_SJIT_Fact_Items as with orders as ( select * from (select upper(\'Myntra_SJIT\') as Marketplace ,upper(CITY) CITY ,SIZE ,upper(BRAND) BRAND ,upper(STATE) STATE ,\"SKU Id\" SKU_ID ,ZIPCODE ,try_cast(DISCOUNT as float) discount ,\"Style ID\" STYLE_ID ,try_to_timestamp(\"FMPU date\") FMPU_Date ,try_to_timestamp(\"Lost date\") Lost_Date ,try_to_timestamp(\"Packed On\") Packed_On ,\"Packet Id\" Packet_ID ,\"Seller Id\" Seller_ID ,try_cast(\"Total Mrp\" as float) MRP_Sales ,try_to_timestamp(\"Created On\") Created_On ,try_to_timestamp(\"Shipped On\") Shipped_On ,REVERSE(SUBSTRING(REVERSE(\"Vendor Article Number\"), CHARINDEX(\'-\', REVERSE(\"Vendor Article Number\")) + 1)) Style_Name ,try_cast(\"Gift Charge\" as float) GIFT_CHARGE ,\"Order Id FK\" Order_ID_FK ,Upper(\"Article Type\") Article_Type ,try_to_timestamp(\"Cancelled On\") cancelled_on ,Upper(\"Courier Code\") Courier_Code ,try_to_timestamp(\"Delivered On\") Delivered_On ,try_cast(\"Final Amount\" as float) Final_Amount ,try_to_timestamp(\"Inscanned On\") Inscanned_On ,upper(\"Order Status\") Order_Status ,try_cast(\"Tax Recovery\" as float) Tax_Recovery ,\"Warehouse Id\" Warehouse_ID ,\"CORE_ITEM_ID\" CORE_ITEM_ID ,\"Order Line Id\" Order_Line_ID ,\"Store Order Id\" Store_Order_ID ,\"Article Type Id\" Article_Type_ID ,try_cast(\"Coupon Discount\" as float) Coupon_discount ,\"Vendor Article Number\" Myntra_SKU_code ,\"Seller Order Id\" Seller_Order_ID ,\"Seller Packe Id\" Seller_Packe_Id ,\"Seller SKU Code\" Seller_SKU_CODE ,try_cast(\"Shipping Charge\" as float) Shipping_Charge ,\"Order Release Id\" Order_Release_ID ,try_to_date(\"RTO Creation date\") RTO_Creation_Date ,Upper(\"Cancellation Reason\") Cancellation_Reason ,\"Seller Warehouse Id\" Seller_Warehouse_ID ,try_to_timestamp(\"Return Creation date\") Return_Creation_Date ,\"Order Tracking Number\" Order_Tracking_Number ,\"Vendor Article Number\" Vendor_Article_Number ,\"Cancellation Reason Id Fk\" Cancellation_Reason_ID_FK ,_AIRBYTE_AB_ID::varchar _AIRBYTE_AB_ID ,_AIRBYTE_EMITTED_AT ,_AIRBYTE_NORMALIZED_AT , row_number() over (partition by \"Order Id FK\" , \"Store Order Id\", \"CORE_ITEM_ID\", \"Order Line Id\" order by _AIRBYTE_EMITTED_AT desc) rw from Elcinco_db.maplemonk.myntra_sjit_sales ) where rw = 1 ), SKU_MASTER AS ( SELECT * FROM ( SELECT skucode, name, brand, category, sub_category, ROW_NUMBER() OVER (PARTITION BY skucode ORDER BY 1) AS rw FROM Elcinco_db.MAPLEMONK.sku_master ) subquery WHERE subquery.rw = 1 ) select fi.* ,Upper(coalesce(p.name, fi.Style_Name)) as PRODUCT_NAME_Final ,Upper(coalesce(p.CATEGORY,fi.Article_Type)) AS Product_Category ,Upper(p.sub_category) as Product_Sub_Category ,upper(coalesce(p.brand, fi.brand)) BRAND_FINAL from orders fi left join SKU_MASTER p on fi.SKU_ID = p.skucode ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from elcinco_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        