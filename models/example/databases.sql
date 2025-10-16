{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table beastlife-wh-474411.maplemonk.D2C_master as select sc.source medium, sc.channel source, sc.utm_campaign, sc.utm_term, order_status, sc.Order_Date, sc.marketplace, sc.customer_id, sc.app_website_flag, new_customer_flag, sc.NAME, sc.EMAIL, sc.PHONE, sc.pincode, sc.ORDER_ID, sc.reference_Code ordeR_name, sc.SKU, sc.PRODUCT_NAME, sc.PRODUCT_CATEGORY, sc.PRODUCT_SUB_CATEGORY, sc.discount_codes, sc.selling_price, sc.quantity, sc.TAX, sc.SHIPPING_PRICE, sc.DISCOUNT, cm.customer_segment, case when (lower(ARRAY_TO_STRING(properties, \',\')) like \'%freebie%\' or lower(ARRAY_TO_STRING(properties, \',\')) like \'%payday%\' or lower(ARRAY_TO_STRING(properties, \',\')) like \'%free shaker%\' or lower(ARRAY_TO_STRING(properties, \',\')) like \'%free tshirt%\' or lower(ARRAY_TO_STRING(properties, \',\')) like \'%free gift%\' or lower(ARRAY_TO_STRING(properties, \',\')) like \'%mrt%\' or lower(ARRAY_TO_STRING(properties, \',\')) like \'%pr%\' or lower(ARRAY_TO_STRING(properties, \',\')) like \'%gift%\') or ((discount_codes like \'%7AJGZ3HJ9DYV%\' or discount_codes like \'%T4RBQB%\') and sku = \'BL003\') then 1 else 0 end freebie_flag from beastlife-wh-474411.maplemonk.beastlife_wh_474411_sales_consolidated as sc left join (select * from beastlife-wh-474411.maplemonk.beastlife_wh_474411_CUSTOMER_MASTER where acquisition_marketplace = \'WEBSITE\') as cm on cm.mm_customer_id = sc.customer_id_final ;",
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
            