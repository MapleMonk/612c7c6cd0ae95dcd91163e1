{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table asaya-data-room-487110.maplemonk.D2C_master as select sc.source, sc.channel, medium, sc.utm_campaign, order_status, sc.Order_Date, sc.ordeR_time, sc.marketplace, sc.customer_id, sc.new_customer_flag, sc.NAME, sc.EMAIL, sc.PHONE, sc.pincode, sc.ORDER_ID, sc.reference_Code ordeR_name, sc.SKU, sc.PRODUCT_NAME, sc.PRODUCT_CATEGORY, sc.PRODUCT_SUB_CATEGORY, sc.discount_codes, sc.selling_price, sc.quantity, sc.TAX, sc.SHIPPING_PRICE, sc.DISCOUNT, cm.customer_segment, sc.cod_pp_flag, sc.app_website_flag, sc.user_id, upper(mcf.account_name) account_name, mcf.ad_name ad_name, mcf.adset_name adset_name, upper(mcf.campaign_name) campaign_name, sc.utm_term, sc.utm_content from asaya-data-room-487110.maplemonk.asaya_data_room_487110_sales_consolidated as sc left join (select * from asaya-data-room-487110.maplemonk.asaya_data_room_487110_CUSTOMER_MASTER where acquisition_marketplace = \'WEBSITE\') as cm on cm.mm_customer_id = sc.customer_id_final left join ( select * from ( select distinct ad_name, ad_id, adset_name, account_name, campaign_name, row_number() over (partition by ad_id order by 1) rw from asaya-data-room-487110.maplemonk.asaya_data_room_487110_MARKETING_CONSOLIDATED where channel = \'FACEBOOK\')where rw = 1 and ad_id is not null) mcf on sc.utm_term = mcf.ad_id ;",
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
            