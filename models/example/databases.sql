{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table MEDMONGERS_DB.MAPLEMONK.MedMongers_Profit_and_Loss_Fact_Items as select customer_id_final, acquisition_date, first_complete_order_date, maple_monk_id_phone, customer_id, SHOP_NAME, coalesce(sc.marketplace,mc.marketplace) marketplace, coalesce(sc.CHANNEL,mc.channel) CHANNEL, SOURCE, cast(ORDER_ID as string) ORDER_ID, cast(sc.reference_code as string)reference_code, PHONE, sc.NAME, sc.brand, EMAIL, SHIPPING_LAST_UPDATE_DATE, sc.COMMONSKU AS SKU, PRODUCT_ID, PRODUCT_NAME, CURRENCY, CITY, State, ORDER_STATUS, coalesce(sc.Order_Date,mc.date) as Order_Date, sc.Order_Date as order_time, QUANTITY, GROSS_SALES_BEFORE_TAX, DISCOUNT, TAX, 0 as SHIPPING_PRICE, SELLING_PRICE, OMS_order_status, SHIPPING_STATUS, FINAL_SHIPPING_STATUS, sc.SALEORDERITEMCODE, SALES_ORDER_ITEM_ID, AWB, cast(Payment_Mode as string) as PAYMENT_GATEWAY, Payment_Mode, COURIER, DISPATCH_DATE, DELIVERED_DATE, DELIVERED_STATUS, RETURN_FLAG, returned_sales, cancelled_quantity, Days_in_Shipment, sc.commonsku AS SKU_CODE, upper(PRODUCT_NAME_FINAL) as PRODUCT_NAME_FINAL, sc.PRODUCT_CATEGORY AS Product_Category, PRODUCT_SUB_CATEGORY, commonsku, WAREHOUSE, new_customer_flag, new_customer_flag_month, tax_rate, sc.cogs * (ifnull(QUANTITY,0)) AS cogs, sc.packaging_cost * (ifnull(QUANTITY,0)) AS packaging_cost, sc.sku_mrp * ifnull(quantity,0) as mrp, div0(ifnull(mc.spend,0) , count(*) over(partition by coalesce(sc.Order_Date,mc.date) ,upper(coalesce(sc.CHANNEL,mc.channel,\'\')) ,upper(coalesce(sc.marketplace,mc.marketplace,\'\') ))) as Marketing_Spend from MEDMONGERS_DB.MAPLEMONK.MedMongers_sales_consolidated sc full outer join ( select date ,upper(ifnull(channel,\'\')) channel ,case when upper(ifnull(channel,\'\')) like \'%AMAZON%\' then \'AMAZON\' when upper(ifnull(channel,\'\')) like \'%FLIPKART%\' then \'FLIPKART\' else \'WEBSITE\' end as marketplace ,sum(ifnull(spend,0)) spend from MEDMONGERS_DB.MAPLEMONK.MEDMONGERS_MARKETING_CONSOLIDATED group by 1,2,3 )mc on sc.order_date = mc.date and upper(ifnull(sc.channel,\'\')) = upper(mc.channel) and upper(mc.marketplace) = upper(sc.marketplace) ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MEDMONGERS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            