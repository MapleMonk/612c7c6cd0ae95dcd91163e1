{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table BUMMER_DB.MAPLEMONK.BUMMER_PandL as WITH SKU_MRP_COGS as ( select * from (select sku_code , try_to_date(start_date,\'DD-MON-YY\') start_Date , try_to_date(end_date,\'DD-MON-YY\') End_date , try_to_double(mrp) mrp , try_to_double(cogs) cogs , row_number() over (partition by sku_code, start_date, end_date order by mrp desc) rw from BUMMER_DB.MAPLEMONK.SKU_MRP_COGS ) where rw=1 ), PACKAGING_COST as ( select * from (select UPPER(CATEGORY) CATEGORY , try_to_date(from_date,\'DD-MON-YY\') start_Date , try_to_date(to_date,\'DD-MON-YY\') End_date , UPPER(PACKAGING_TYPE) PACKAGING_TYPE , upper(type) COST_TYPE , UPPER(MARKETPLACE) MARKETPLACE , try_to_double(charges) Packaging_Cost , try_to_double(max_quantity) MAX_QUANTITY , try_to_double(min_quantity) MIN_QUANTITY , row_number() over (partition by upper(category), try_to_date(from_date,\'DD-MON-YY\'), try_to_date(to_date,\'DD-MON-YY\'), UPPER(MARKETPLACE), UPPER(PACKAGING_TYPE), try_to_double(max_quantity), try_to_double(min_quantity) order by 1 desc) rw from BUMMER_DB.MAPLEMONK.PACKAGING_FEE ) where rw=1 ), RATE_CARD as ( SELECT * FROM ( SELECT Upper(COURIER) COURIER ,UPPER(ZONE) ZONE ,TRY_TO_DOUBLE(WEIGHT_START) WEIGHT_START ,TRY_TO_DOUBLE(WEIGHT_END) WEIGHT_END ,TRY_TO_DOUBLE(forward_rate) FORWARD_RATE ,TRY_TO_DOUBLE(reverse_rate) REVERSE_RATE ,TRY_TO_DOUBLE(cod_charges ) COD_CHARGES ,row_number() over (partition by UPPER(COURIER), UPPER(ZONE), TRY_TO_DOUBLE(WEIGHT_START),TRY_TO_DOUBLE(WEIGHT_END) order by 1) rw FROM BUMMER_DB.MAPLEMONK.FINAL_ZONE_RATE_CARD ) where rw =1 ), PINCODE_ZONE as ( SELECT * FROM ( SELECT UPPER(ZONE) ZONE ,\"Delivery Pincode\" PINCODE ,row_number() over (partition by UPPER(ZONE), UPPER(\"Delivery Pincode\") order by 1) rw FROM BUMMER_DB.MAPLEMONK.PINCODE_ZONE_MAPPING ) where rw =1 ), COURIER_MAPPING as ( select * from (select upper(courier_name) courier_name ,upper(final_courier_name) final_courier_name , row_number() over (partition by upper(courier_name), upper(final_courier_name) order by 1) rw from BUMMER_DB.MAPLEMONK.COURIER_NAME_MAPPING ) where rw=1 ), PAYMENT_COSTS as ( select * from (select upper(partner) PAYMENT_GATEWAY ,upper(PAYMENT_MODE) PAYMENT_MODE ,Upper(DETAIL) TYPE , try_to_date(from_date,\'DD-MON-YY\') start_Date , try_to_date(to_date,\'DD-MON-YY\') End_date , try_to_double(replace(charges,\'%\',\'\')) PAYMENT_GATEWAY_COST , row_number() over (partition by upper(partner), upper(PAYMENT_MODE), try_to_date(from_date,\'DD-MON-YY\'), try_to_date(to_date,\'DD-MON-YY\') order by 1) rw from BUMMER_DB.MAPLEMONK.PAYMENT_COSTS ) where rw=1 ), AWB_LOGISTICS as ( Select AWB ,PINCODE_ZONE.ZONE ,awb.COURIER as awb_COURIER ,COURIER_MAPPING.FINAL_COURIER_NAME ,reference_code as awb_reference_code ,TOTAL_AWB_WEIGHT ,RATE_CARD.FORWARD_RATE ,RATE_CARD.REVERSE_RATE ,RATE_CARD.COD_CHARGES from (SELECT AWB ,PINCODE ,COURIER ,reference_code ,sum(quantity) QUANTITY ,sum(weight)/1000 TOTAL_AWB_WEIGHT from BUMMER_DB.MAPLEMONK.BUMMER_DB_sales_consolidated group by 1,2,3,4) AWB left join PINCODE_ZONE on PINCODE_ZONE.PINCODE = AWB.PINCODE left join COURIER_MAPPING on upper(AWB.courier) = COURIER_MAPPING.COURIER_NAME left join RATE_CARD ON COURIER_MAPPING.FINAL_COURIER_NAME = RATE_CARD.COURIER AND PINCODE_ZONE.ZONE = RATE_CARD.ZONE AND AWB.TOTAL_AWB_WEIGHT > RATE_CARD.WEIGHT_START AND AWB.TOTAL_AWB_WEIGHT <= RATE_CARD.WEIGHT_END ), EXTERNAL_PACKAGING as ( Select reference_code ,ORDER_QUANTITY ,PACKAGING_CATEGORY ,external_packaging_cost.Packaging_Cost EXTERNAL_PACKAGING_COST from (SELECT reference_code ,order_Date ,case when lower(marketplace) like \'%shopify%\' then \'SHOPIFY\' when lower(marketplace) like \'%amazon%\' then \'AMAZON\' when lower(marketplace) like \'%myntra%\' then \'MYNTRA\' when lower(marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(marketplace) like \'%ajio%\' then \'AJIO\' when lower(marketplace) like \'%flipkart%\' then \'FLIPKART\' else marketplace end MARKETPLACE ,PACKAGING_CATEGORY ,sum(quantity) ORDER_QUANTITY from BUMMER_DB.MAPLEMONK.BUMMER_DB_sales_consolidated group by 1,2,3,4 ) ORDERS left join(select * from PACKAGING_COST where lower(packaging_type) like \'%external%\') external_packaging_cost on ORDERS.MARKETPLACE = external_packaging_cost.MARKETPLACE AND ( case when not(lower(orders.marketplace) like \'%shopify%\') then \'ALL\' else ORDERS.PACKAGING_CATEGORY end) = external_packaging_cost.CATEGORY AND ORDERS.ORDER_QUANTITY >= external_packaging_cost.MIN_QUANTITY AND ORDERS.ORDER_QUANTITY <= external_packaging_cost.MAX_QUANTITY AND ORDERS.order_Date::date >= external_packaging_cost.START_DATE AND ORDERS.order_Date::date <= external_packaging_cost.END_DATE ), Returns as ( select RFI.* ,skumaster.commonsku SKU_CODE ,SKUMASTER.CATEGORY PRODUCT_CATEGORY ,SKUMASTER.SUB_CATEGORY PRODUCT_SUB_CATEGORY ,SKUMASTER.GENDER PRODUCT_GENDER ,SKUMASTER.SIZE SIZE ,UPPER(coalesce(SKUMASTER.NAME, ITEM_NAME)) PRODUCT_NAME_FINAL ,SKUMASTER.PACKAGING_CATEGORY ,SKUMASTER.WEIGHT UNIT_WEIGHT from BUMMER_DB.MAPLEMONK.BUMMER_DB_unicommerce_returns_fact_items RFI left join BUMMER_DB.MAPLEMONK.FINAL_SKU_MASTER SKUMASTER on UPPER(RFI.ITEMSKU) = SKUMASTER.MARKETPLACE_SKU ), Sales_CTE as ( select a.SALEORDERITEMCODE ,a.source as Marketing_Source ,a.channel as Marketing_Channel ,a.marketplace ,a.order_date as Date ,a.order_id ,a.reference_code ,a.shop_name ,a.return_flag ,a.new_customer_flag ,a.customer_id_final ,a.payment_mode ,a.payment_gateway ,a.pincode ,a.awb ,a.courier ,AWB_LOGISTICS_COSTS.FINAL_COURIER_NAME ,AWB_LOGISTICS_COSTS.ZONE ,a.order_Status ,a.shipping_status ,a.final_shipping_status ,case when (lower(a.order_Status) like \'%cancel%\' or lower(a.final_shipping_status) like \'%cancel%\') then \'CANCELLED\' else upper(coalesce(final_shipping_status,order_status)) end final_status ,a.sku ,a.sku_code ,a.product_name_final PRODUCT_NAME_FINAL ,a.product_sub_category product_sub_category ,a.product_category PRODUCT_CATEGORY ,a.packaging_category ,a.product_gender ,a.size ,a.weight/1000 unit_weight ,a.quantity ,a.quantity*a.weight/1000 total_weight ,AWB_LOGISTICS_COSTS.TOTAL_AWB_WEIGHT ,MRP_COGS.MRP*quantity MRP ,a.selling_price Gross_sale ,a.shipping_price shipping_price ,a.tax tax ,case when lower(coalesce(final_status,\'1\')) not in (\'cancelled\',\'rto\', \'returned\', \'return\') then MRP_COGS.cogs*a.quantity else 0 end as COGS ,case when not(lower(final_status) like \'%cancel%\') then INTERNAL_PACKAGING_COST.Packaging_Cost*a.quantity else 0 end INTERNAL_PACKAGING ,case when not(lower(final_status) like \'%cancel%\') then div0(EXTERNAL_PACKAGING.EXTERNAL_PACKAGING_COST, count(1) over (partition by a.reference_code, a.packaging_category)) else 0 end EXTERNAL_PACKAGING_COST ,case when a.new_customer_flag = \'Repeat\' then LAG(a.order_date) IGNORE NULLS OVER (partition by a.customer_id_final ORDER BY a.order_date) end previous_date ,datediff(day,previous_date,a.order_Date) days_from_last_order ,div0(AWB_LOGISTICS_COSTS.FORWARD_RATE, count(1) over (partition by a.awb)) AS FORWARD_LOGISTICS_COST ,case when lower(a.final_shipping_status) like any (\'%rto%\', \'%return%\') then div0(AWB_LOGISTICS_COSTS.REVERSE_RATE, count(1) over (partition by a.awb)) else 0 end AS REVERSE_LOGISTICS_COST ,case when lower(a.payment_mode) like \'%cod%\' then DIV0(AWB_LOGISTICS_COSTS.COD_CHARGES, count(1) over (partition by a.awb)) else 0 end AS COD_CHARGES ,div0(ifnull(GOOGLE_SPEND.spend,0), count(1) over (partition by a.order_Date::date, a.channel)) as Paid_Marketing_Google ,div0(ifnull(FB_SPEND.spend,0), count(1) over (partition by a.order_Date::date, a.channel)) as Paid_Marketing_Facebook ,div0(ifnull(AMAZON_SPEND.spend,0), count(1) over (partition by a.order_Date::date, a.channel)) as Paid_Marketing_Amazon from BUMMER_DB.MAPLEMONK.BUMMER_DB_sales_consolidated a left join SKU_MRP_COGS MRP_COGS on lower(replace(MRP_COGS.sku_code,\' \',\'\')) = lower(replace(a.sku_code,\' \',\'\')) AND to_date(a.order_date)::date >= MRP_COGS.start_date AND to_date(a.order_date)::date <= MRP_COGS.end_date left join (select date, sum(spend) spend from BUMMER_DB.MAPLEMONK.BUMMER_DB_MARKETING_CONSOLIDATED where lower(channel) like \'%google%\' group by date) GOOGLE_SPEND on GOOGLE_SPEND.date = a.order_Date::date AND lower(case when lower(a.channel) like \'%google%\' then \'google\' end) like \'%google%\' left join (select date, sum(spend) spend from BUMMER_DB.MAPLEMONK.BUMMER_DB_MARKETING_CONSOLIDATED where lower(channel) like \'%facebook%\' group by date) FB_SPEND on FB_SPEND.date = a.order_Date::date AND lower(case when lower(a.channel) like any (\'%facebook%\', \'%insta%\', \'%ig%\',\'%meta%\') then \'facebook\' end) like \'%facebook%\' left join (select date, sum(spend) spend from BUMMER_DB.MAPLEMONK.BUMMER_DB_MARKETING_CONSOLIDATED where lower(channel) like \'%amazon%\' group by date) AMAZON_SPEND on AMAZON_SPEND.date = a.order_Date::date and lower(a.channel) like \'%amazon%\' left join (select * from AWB_LOGISTICS where awb is not null) AWB_LOGISTICS_COSTS on a.awb = AWB_LOGISTICS_COSTS.awb AND a.courier = AWB_LOGISTICS_COSTS.awb_courier AND a.reference_code =AWB_LOGISTICS_COSTS.awb_reference_code left join (select * from PACKAGING_COST where lower(packaging_type) like \'%internal%\') INTERNAL_PACKAGING_COST on a.packaging_category = INTERNAL_PACKAGING_COST.category AND a.order_Date::date >= INTERNAL_PACKAGING_COST.START_DATE AND a.order_Date::date <= INTERNAL_PACKAGING_COST.END_DATE left join EXTERNAL_PACKAGING on a.reference_code = EXTERNAL_PACKAGING.reference_code and a.packaging_category = EXTERNAL_PACKAGING.PACKAGING_CATEGORY ) select SALEORDERITEMCODE ,MARKETING_SOURCE ,MARKETING_CHANNEL ,MARKETPLACE ,DATE ,ORDER_ID ,REFERENCE_CODE ,SHOP_NAME ,RETURN_FLAG ,NEW_CUSTOMER_FLAG ,CUSTOMER_ID_FINAL ,PAYMENT_MODE ,PAYMENT_GATEWAY ,PINCODE ,AWB ,COURIER ,FINAL_COURIER_NAME ,ZONE ,ORDER_STATUS ,SHIPPING_STATUS ,FINAL_SHIPPING_STATUS ,FINAL_STATUS ,SKU ,SKU_CODE ,PRODUCT_NAME_FINAL ,PRODUCT_SUB_CATEGORY ,PRODUCT_CATEGORY ,PACKAGING_CATEGORY ,PRODUCT_GENDER ,SIZE ,UNIT_WEIGHT ,QUANTITY ,TOTAL_WEIGHT ,TOTAL_AWB_WEIGHT ,MRP ,GROSS_SALE ,SHIPPING_PRICE ,TAX ,COGS ,INTERNAL_PACKAGING ,EXTERNAL_PACKAGING_COST ,PREVIOUS_DATE ,DAYS_FROM_LAST_ORDER ,FORWARD_LOGISTICS_COST ,REVERSE_LOGISTICS_COST ,COD_CHARGES ,PAID_MARKETING_GOOGLE ,PAID_MARKETING_FACEBOOK ,PAID_MARKETING_AMAZON ,null as RETURN_ORDERITEMCODE ,null as RETURN_ORDER_ID ,null as RETURN_REFERENCE_CODE ,null as RETURNED_QUANTITY ,null as TOTAL_RETURN_AMOUNT ,null as RETURN_TAX ,null as RETURN_AMOUNT_WITHOUT_TAX ,null as RETURN_DISPLAYCODE ,null as RETUNR_INVENTORY_TYPE ,null as RETURN_STATUS ,null as INVENTORY_RECEIVED_DATE ,null as RETURN_TYPE ,null as RETURN_PROVIDER_SHIPPING_STATUS ,null as RETURN_INVOICE_DISPLAY_CODE from Sales_CTE union all select null as SALEORDERITEMCODE ,SOURCE ,MARKETING_CHANNEL ,MARKETPLACE ,RETURN_COMPLETE_DATE ,null as ORDER_ID ,null as REFERENCE_CODE ,MARKETPLACE ,null as RETURN_FLAG ,null as NEW_CUSTOMER_FLAG ,null as CUSTOMER_ID_FINAL ,null as PAYMENT_MODE ,null as PAYMENT_GATEWAY ,null as PINCODE ,null as AWB ,RETURN_COURIER ,null as FINAL_COURIER_NAME ,null as ZONE ,case when lower(RETURN_STATUS) like \'%return%\' then RETURN_STATUS else concat(\'RETURN\',RETURN_STATUS) end ORDER_STATUS ,case when lower(RETURN_STATUS) like \'%return%\' then RETURN_STATUS else concat(\'RETURN\',RETURN_STATUS) end SHIPPING_STATUS ,case when lower(RETURN_STATUS) like \'%return%\' then RETURN_STATUS else concat(\'RETURN\',RETURN_STATUS) end FINAL_SHIPPING_STATUS ,case when lower(RETURN_STATUS) like \'%return%\' then RETURN_STATUS else concat(\'RETURN\',RETURN_STATUS) end FINAL_STATUS ,ITEMSKU ,SKU_CODE ,PRODUCT_NAME_FINAL ,PRODUCT_SUB_CATEGORY ,PRODUCT_CATEGORY ,PACKAGING_CATEGORY ,PRODUCT_GENDER ,SIZE ,UNIT_WEIGHT ,null as QUANTITY ,null as TOTAL_WEIGHT ,null as TOTAL_AWB_WEIGHT ,null as MRP ,null as GROSS_SALE ,null as SHIPPING_PRICE ,null as TAX ,null as COGS ,null as INTERNAL_PACKAGING ,null as EXTERNAL_PACKAGING_COST ,null as PREVIOUS_DATE ,null as DAYS_FROM_LAST_ORDER ,null as FORWARD_LOGISTICS_COST ,null as REVERSE_LOGISTICS_COST ,null as COD_CHARGES ,null as PAID_MARKETING_GOOGLE ,null as PAID_MARKETING_FACEBOOK ,null as PAID_MARKETING_AMAZON ,SALEORDERITEMCODE as RETURN_ORDERITEMCODE ,ORDER_ID as RETURN_ORDER_ID ,REFERENCE_CODE as RETURN_REFERENCE_CODE ,RETURNED_QUANTITY ,TOTAL_RETURN_AMOUNT ,RETURN_TAX ,RETURN_AMOUNT_WITHOUT_TAX ,RETURN_DISPLAYCODE ,INVENTORY_TYPE RETUNR_INVENTORY_TYPE ,RETURN_STATUS ,INVENTORY_RECEIVED_DATE ,RETURN_TYPE ,RETURN_PROVIDER_SHIPPING_STATUS ,RETURN_INVOICE_DISPLAY_CODE from returns;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BUMMER_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        