{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table select_db.maplemonk.select_db_pandl as with sales_data as ( select ORDER_ID ,REFERENCE_CODE ,SALEORDERITEMCODE ,SALES_ORDER_ITEM_ID ,PHONE ,NAME ,EMAIL ,MAPLE_MONK_ID_PHONE ,CUSTOMER_ID ,CUSTOMER_ID_FINAL ,ACQUSITION_DATE ,FIRST_COMPLETE_ORDER_DATE ,NEW_CUSTOMER_FLAG ,NEW_CUSTOMER_FLAG_MONTH ,ACQUISITION_PRODUCT ,ACQUISITION_CHANNEL ,ACQUISITION_MARKETPLACE ,SHOP_NAME ,MARKETPLACE ,CHANNEL ,SOURCE ,SKU ,PRODUCT_ID ,PRODUCT_NAME ,SKU_CODE ,coalesce(SM.productname,SC.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,coalesce(SM.category,SC.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,coalesce(SM.sub_category,SC.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY ,CURRENCY ,CITY ,STATE ,ORDER_DATE ,SHIPPING_LAST_UPDATE_DATE ,ORDER_STATUS ,OMS_ORDER_STATUS ,SHIPPING_STATUS ,FINAL_SHIPPING_STATUS ,AWB ,COURIER ,DISPATCH_DATE ,DELIVERED_DATE ,DELIVERED_STATUS ,DAYS_IN_SHIPMENT ,WAREHOUSE ,PAYMENT_GATEWAY ,PAYMENT_MODE ,RETURN_FLAG ,coalesce(SM.commonskuid, sc.sku_code) commonskuid ,Quantity ,GROSS_SALES_BEFORE_TAX ,DISCOUNT ,TAX ,SHIPPING_PRICE ,SELLING_PRICE ,RETURNED_QUANTITY ,RETURNED_SALES ,CANCELLED_QUANTITY from SELECT_DB.MAPLEMONK.SELECT_db_sales_consolidated sc left join (select * from (select marketplace_sku skucode , commonskuid , name productname , category , sub_category , row_number() over (partition by lower(marketplace_sku) order by 1) rw from SELECT_DB.maplemonk.select_db_sku_master ) where rw = 1 ) SM on lower(sc.sku_code) = lower(SM.skucode) ) select SC.ORDER_ID ,SC.REFERENCE_CODE ,SC.SALEORDERITEMCODE ,SC.SALES_ORDER_ITEM_ID ,SC.PHONE ,SC.NAME ,SC.EMAIL ,SC.MAPLE_MONK_ID_PHONE ,SC.CUSTOMER_ID ,SC.CUSTOMER_ID_FINAL ,SC.ACQUSITION_DATE ,SC.FIRST_COMPLETE_ORDER_DATE ,SC.NEW_CUSTOMER_FLAG ,SC.NEW_CUSTOMER_FLAG_MONTH ,SC.ACQUISITION_PRODUCT ,SC.ACQUISITION_CHANNEL ,SC.ACQUISITION_MARKETPLACE ,SC.SHOP_NAME ,SC.MARKETPLACE ,coalesce(sc.CHANNEL, google_spends.channel, fb_spends.channel, amazon_spends.channel) channel ,SC.SOURCE ,SC.SKU ,SC.PRODUCT_ID ,SC.PRODUCT_NAME ,SC.SKU_CODE ,SC.PRODUCT_NAME_FINAL ,SC.PRODUCT_CATEGORY ,SC.PRODUCT_SUB_CATEGORY ,SC.CURRENCY ,SC.CITY ,SC.STATE ,coalesce(sc.order_date::Date,Google_spends.date, fb_spends.date, amazon_spends.date, return.return_date::date) Order_Date ,SC.SHIPPING_LAST_UPDATE_DATE ,SC.ORDER_STATUS ,SC.OMS_ORDER_STATUS ,SC.SHIPPING_STATUS ,SC.FINAL_SHIPPING_STATUS ,SC.AWB ,SC.COURIER ,SC.DISPATCH_DATE ,SC.DELIVERED_DATE ,SC.DELIVERED_STATUS ,SC.DAYS_IN_SHIPMENT ,SC.WAREHOUSE ,SC.PAYMENT_GATEWAY ,SC.PAYMENT_MODE ,SC.RETURN_FLAG ,sc.commonskuid ,coalesce(pcm.commonskuid_child, SC.sku_code) SKU_CODE_CHILD ,case when pcm.commonskuid_child is null then sc.quantity else ifnull(pcm.qty,0)*sc.quantity end as QUANTITY_CHILD ,case when pcm.commonskuid_child is null then sc.RETURNED_QUANTITY else ifnull(pcm.qty,0)*sc.RETURNED_QUANTITY end as RETURNED_QUANTITY_CHILD ,case when pcm.commonskuid_child is null then sc.CANCELLED_QUANTITY else ifnull(pcm.qty,0)*sc.CANCELLED_QUANTITY end as CANCELLED_QUANTITY_CHILD ,div0(QUANTITY,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) Quantity ,div0(GROSS_SALES_BEFORE_TAX,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) GROSS_SALES_BEFORE_TAX ,div0(DISCOUNT, count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) DISCOUNT ,div0(TAX,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) TAX ,div0(SHIPPING_PRICE,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) SHIPPING_PRICE ,div0(SELLING_PRICE,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) SELLING_PRICE ,div0(sc.RETURNED_QUANTITY,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) RETURNED_QUANTITY ,div0(sc.RETURNED_SALES,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) RETURNED_SALES ,div0(CANCELLED_QUANTITY,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) CANCELLED_QUANTITY ,COGS_MAP.cogs ,COGS_MAP.product_cost ,COGS_MAP.PACKING_MILE_COST ,COGS_MAP.TOTAL_COST_EXCL_TAX ,div0(ifnull(google_spends.marketing_spend,0),count(1) over (partition by coalesce(sc.order_date::Date,Google_spends.date, fb_spends.date, amazon_spends.date), lower(coalesce(sc.CHANNEL, google_spends.channel, fb_spends.channel, amazon_spends.channel)))) Google_Marketing_Spend ,div0(ifnull(fb_spends.marketing_spend,0),count(1) over (partition by coalesce(sc.order_date::Date,Google_spends.date, fb_spends.date, amazon_spends.date), lower(coalesce(sc.CHANNEL, google_spends.channel, fb_spends.channel, amazon_spends.channel)))) Facebook_Marketing_Spend ,div0(ifnull(amazon_spends.marketing_spend,0),count(1) over (partition by coalesce(sc.order_date::Date,Google_spends.date, fb_spends.date, amazon_spends.date), lower(coalesce(sc.CHANNEL, google_spends.channel, fb_spends.channel, amazon_spends.channel)))) Amazon_Marketing_Spend from sales_data sc left join (select * from (select replace(commonskuid,\'`\',\'\') commonskuid , replace(skucode_child,\'`\',\'\') commonskuid_child , qty::float qty , row_number() over (partition by lower(commonskuid), lower(skucode_child) order by 1) rw from SELECT_DB.MAPLEMONK.mapping_sku_mapping_parent_child ) where rw=1) PCM on lower(PCM.commonskuid) = lower(SC.commonskuid) left join (select * from (select replace(skucode_child,\'`\',\'\') commonskuid_child , cogs::float cogs , product_cost::float product_cost , packing_mile_cost::float packing_mile_cost , total_cost_excl_tax::float total_cost_excl_tax , row_number() over (partition by lower(skucode_child) order by 1) rw from SELECT_DB.MAPLEMONK.mapping_sku_mrp_cogs ) where rw=1) COGS_MAP on lower(COGS_MAP.commonskuid_child) = lower(PCM.commonskuid_child) FULL OUTER JOIN (select date, channel, sum(spend) marketing_spend from SELECT_DB.MAPLEMONK.select_db_marketing_consolidated where lower(channel) like \'%google%\' group by 1,2 ) Google_spends on sc.order_date::date = Google_spends.Date and lower(sc.channel) = lower(Google_spends.channel) FULL OUTER JOIN (select date, channel, sum(spend) marketing_spend from SELECT_DB.MAPLEMONK.select_db_marketing_consolidated where lower(channel) like \'%facebook%\' group by 1,2 ) fb_spends on sc.order_date::date = fb_spends.Date and lower(sc.channel) = lower(fb_spends.channel) FULL OUTER JOIN (select date, channel, sum(spend) marketing_spend from SELECT_DB.MAPLEMONK.select_db_marketing_consolidated where lower(channel) like \'%amazon%\' group by 1,2 ) amazon_spends on sc.order_date::date = amazon_spends.Date and lower(sc.channel) = lower(amazon_spends.channel);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SELECT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        