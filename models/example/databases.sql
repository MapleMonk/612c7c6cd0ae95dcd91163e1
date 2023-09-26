{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table rubans_db.maplemonk.Rubans_Myntra_SJIT_Fact_Items as select * from (select upper(\'Myntra_SJIT\') as Marketplace ,upper(CITY) CITY ,SIZE ,upper(BRAND) BRAND ,upper(STATE) STATE ,\"SKU Id\" SKU_ID ,ZIPCODE ,try_cast(DISCOUNT as float) discount ,\"Style ID\" STYLE_ID ,try_to_timestamp(\"FMPU date\", \'mm/dd/yy HH24:MI\') FMPU_Date ,try_to_timestamp(\"Lost date\", \'mm/dd/yy HH24:MI\') Lost_Date ,try_to_timestamp(\"Packed On\", \'mm/dd/yy HH24:MI\') Packed_On ,\"Packet Id\" Packet_ID ,\"Seller Id\" Seller_ID ,try_cast(\"Total Mrp\" as float) MRP_Sales ,try_to_timestamp(\"Created On\", \'mm/dd/yy HH24:MI\') Created_On ,try_to_timestamp(\"Shipped On\", \'mm/dd/yy HH24:MI\') Shipped_On ,upper(\"Style Name\") Style_Name ,try_cast(\"Gift Charge\" as float) GIFT_CHARGE ,\"Order Id FK\" Order_ID_FK ,Upper(\"Article Type\") Article_Type ,try_to_timestamp(\"Cancelled On\", \'mm/dd/yy HH24:MI\') cancelled_on ,Upper(\"Courier Code\") Courier_Code ,try_to_timestamp(\"Delivered On\", \'mm/dd/yy HH24:MI\') Delivered_On ,try_cast(\"Final Amount\" as float) Final_Amount ,try_to_timestamp(\"Inscanned On\") Inscanned_On ,upper(\"Order Status\") Order_Status ,try_cast(\"Tax Recovery\" as float) Tax_Recovery ,\"Warehouse Id\" Warehouse_ID ,\"CORE_ITEM_ID\" CORE_ITEM_ID ,\"Order Line Id\" Order_Line_ID ,\"Store Order Id\" Store_Order_ID ,\"Article Type Id\" Article_Type_ID ,try_cast(\"Coupon Discount\" as float) Coupon_discount ,\"Myntra SKU Code\" Myntra_SKU_code ,\"Seller Order Id\" Seller_Order_ID ,\"Seller Packe Id\" Seller_Packe_Id ,\"Seller SKU Code\" Seller_SKU_CODE ,try_cast(\"Shipping Charge\" as float) Shipping_Charge ,\"Order Release Id\" Order_Release_ID ,try_to_date(\"RTO Creation date\", \'mm/dd/yy HH24:MI\') RTO_Creation_Date ,Upper(\"Cancellation Reason\") Cancellation_Reason ,\"Seller Warehouse Id\" Seller_Warehouse_ID ,try_to_timestamp(\"Return Creation date\", \'mm/dd/yy HH24:MI\') Return_Creation_Date ,\"Order Tracking Number\" Order_Tracking_Number ,\"Vendor Article Number\" Vendor_Article_Number ,\"Cancellation Reason Id Fk\" Cancellation_Reason_ID_FK ,_AIRBYTE_AB_ID ,_AIRBYTE_EMITTED_AT ,_AIRBYTE_NORMALIZED_AT ,_AIRBYTE_RUBANS_MYNTRA_SJIT_ORDERS_HASHID , row_number() over (partition by \"Order Id FK\" , \"Store Order Id\", \"CORE_ITEM_ID\", \"Order Line Id\" order by _AIRBYTE_EMITTED_AT desc) rw from rubans_db.maplemonk.rubans_myntra_sjit_orders ) where rw = 1;",
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
                        