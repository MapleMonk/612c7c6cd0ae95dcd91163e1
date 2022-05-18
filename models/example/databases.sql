{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table sales_consolidated_femora as with returns as ( select \"Order ID\" as id1 from femora_mp_orders_flipkart_balancecart where \"Event Type\" = \'Return\' union all select \"Order ID\" from femora_mp_orders_flipkart_shiva where \"Event Type\" = \'Return\' union all select \"Order ID\" from femora_mp_orders_flipkart_swd where \"Event Type\" = \'Return\' ) SELECT \'Amazon_IN\' AS SHOP_NAME , ORDER_ID ,NULL AS ORDER_NAME ,NULL AS CUSTOMER_ID ,NULL AS LINE_ITEM_ID ,SKU ,MARKETPLACE_SKU_ID , PRODUCT_ID , PRODUCT_NAME , PRODUCT_CATEGORY , PRODUCT_SUB_CATEGORY , PRODUCT_MRP ,CURRENCY , IS_RETURN , CITY , upper(STATE) ,NULL AS CATEGORY , ORDER_STATUS , ORDER_TIMESTAMP , LINE_ITEM_SALES , SHIPPING_PRICE , QUANTITY ,TAX ,DISCOUNT ,NET_SALES ,\'Amazon\' AS SOURCE ,NULL AS LANDING_UTM_MEDIUM ,NULL AS LANDING_UTM_SOURCE ,NULL AS LANDING_UTM_CAMPAIGN ,NULL AS REFERRING_UTM_MEDIUM ,NULL AS REFERRING_UTM_SOURCE ,NULL AS LANDING_UTM_CHANNEL ,NULL AS REFERRING_UTM_CHANNEL ,NULL AS FINAL_UTM_CHANNEL ,NULL AS CUSTOMER_FLAG ,NULL AS NEW_CUSTOMER_FLAG ,NULL AS ACQUISITION_CHANNEL ,NULL AS ACQUISITION_PRODUCT ,SHIPPING_TAX ,SHIP_PROMOTION_DISCOUNT ,GIFT_WRAP_PRICE ,GIFT_WRAP_TAX , NULL AS TCS , NULL AS PINCODE , NULL AS CUSTOMER_PHONE , NULL AS CUSTOMER_EMAIL , NULL AS FULFILLMENT_TYPE ,asp._airbyte_emitted_at from maplemonk.maplemonk.FACT_ITEMS_FEMORA asp union all select case when \"Is Shopsy Order?\"=FALSE then \'Flipkart Balancecart\' else \'SHOPSY\' end as shop_name, \"Order ID\", NULL as order_name, NULL as customer_id, \"Order Item ID\", SKU, FSN, sku.sku_id AS PRODUCT_ID ,sku.sku_name AS PRODUCT_NAME ,sku.category as PRODUCT_CATEGORY ,sku.\"Sub Category\" as PRODUCT_SUB_CATEGORY ,sku.mrp as PRODUCT_MRP ,\'INR\', case when r.id1 is not NULL then 1 else 0 end IS_Return, NULL, upper(\"Customer\'s Delivery State\"), NULL, \"Event Type\", try_cast(\"Order Date\" as timestamp), \"Price after discount (Price before discount-Total discount)\" + \"Total Discount\", \"Shipping Charges\", NULL, \"IGST Amount\", -1*\"Total Discount\", \"Price after discount (Price before discount-Total discount)\", \'Flipkart Balancecart\', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \"TCS SGST Rate\" , \"Customer\'s Delivery Pincode\", NULL, NULL, \"Fulfilment Type\", asp._airbyte_emitted_at from femora_mp_orders_flipkart_balancecart asp left join maplemonk.maplemonk.femora_sku_master sku on asp.sku=sku.sku_id left join returns r on id1 = \"Order ID\" where \"Event Type\" <> \'Return\' union all select case when \"Is Shopsy Order?\"=FALSE then \'Flipkart Shiva\' else \'SHOPSY\' end as shop_name, \"Order ID\", NULL as order_name, NULL as customer_id, \"Order Item ID\", SKU, FSN, sku.sku_id AS PRODUCT_ID ,sku.sku_name AS PRODUCT_NAME ,sku.category as PRODUCT_CATEGORY ,sku.\"Sub Category\" as PRODUCT_SUB_CATEGORY ,sku.mrp as PRODUCT_MRP ,\'INR\', case when r.id1 is not NULL then 1 else 0 end IS_Return, NULL, upper(\"Customer\'s Delivery State\"), NULL, \"Event Type\", to_date(\"Order Date\",\'DD/MM/YYYY\'), \"Price after discount (Price before discount-Total discount)\" + \"Total Discount\", \"Shipping Charges\", NULL, \"IGST Amount\", -1*\"Total Discount\", \"Price after discount (Price before discount-Total discount)\", \'Flipkart Shiva\', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \"TCS SGST Rate\" , \"Customer\'s Delivery Pincode\", NULL, NULL, \"Fulfilment Type\", asp._airbyte_emitted_at from femora_mp_orders_flipkart_shiva asp left join maplemonk.maplemonk.femora_sku_master sku on asp.sku=sku.sku_id left join returns r on id1 = \"Order ID\" where \"Event Type\" <> \'Return\' union all select case when \"Is Shopsy Order?\"=FALSE then \'Flipkart SWD\' else \'SHOPSY\' end as shop_name, \"Order ID\", NULL as order_name, NULL as customer_id, \"Order Item ID\", SKU, FSN, sku.sku_id AS PRODUCT_ID ,sku.sku_name AS PRODUCT_NAME ,sku.category as PRODUCT_CATEGORY ,sku.\"Sub Category\" as PRODUCT_SUB_CATEGORY ,sku.mrp as PRODUCT_MRP ,\'INR\', case when r.id1 is not NULL then 1 else 0 end IS_Return, NULL, upper(\"Customer\'s Delivery State\"), NULL, \"Event Type\", try_cast(\"Order Date\" as timestamp), \"Price after discount (Price before discount-Total discount)\" + \"Total Discount\", \"Shipping Charges\", NULL, \"IGST Amount\", -1*\"Total Discount\", \"Price after discount (Price before discount-Total discount)\", \'Flipkart SWD\', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \"TCS SGST Rate\" , \"Customer\'s Delivery Pincode\", NULL, NULL, \"Fulfilment Type\", asp._airbyte_emitted_at from femora_mp_orders_flipkart_swd asp left join maplemonk.maplemonk.femora_sku_master sku on asp.sku=sku.sku_id left join returns r on id1 = \"Order ID\" where \"Event Type\" <> \'Return\' union all select \'Meesho SWD\', \"Sub Order No\", NULL, NULL, \"Sub Order No\", SKU, SKU, NULL, \"Product Name\", NULL, NULL, NULL, \'INR\', case when \"Reason for Credit Entry\" = \'Return\' then 1 else 0 end as IS_RETURN, NULL, upper(\"Reseller State\"), NULL, \"Reason for Credit Entry\", to_date(\"Order Date\",\'DD MON, YYYY\'), \"Total Sales Amount (Incl. GST + Commission)\", \"Delivery Charge(excl Gst)\", Quantity, \"GST Payable on Product\", \"Discount to Reseller (Incl GST and Commision)\", \"Settlement Amount to Supplier\", \'Meesho SWD\', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \"GST on delivery charges\", NULL, NULL, NULL, \"TCS (0.5% CGST+0.5% SGST)\", NULL, NULL, NULL, NULL, a._airbyte_emitted_at from femora_mp_orders_meesho_balancecart a where \"Total Sales Amount (Incl. GST + Commission)\">=0 union all select \'Meesho SWD\', \"Sub Order No\", NULL, NULL, \"Sub Order No\", SKU, SKU, NULL, \"Product Name\", NULL, NULL, NULL, \'INR\', case when \"Reason for Credit Entry\" = \'Return\' then 1 else 0 end as IS_RETURN, NULL, upper(\"Reseller State\"), NULL, \"Reason for Credit Entry\", to_date(\"Order Date\",\'DD MON, YYYY\'), \"Total Sales Amount (Incl. GST + Commission)\", \"Delivery Charge(excl Gst)\", Quantity, \"GST Payable on Product\", \"Discount to Reseller (Incl GST and Commision)\", \"Settlement Amount to Supplier\", \'Meesho SWD\', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \"GST on delivery charges\", NULL, NULL, NULL, \"TCS (0.5% CGST+0.5% SGST)\", NULL, NULL, NULL, NULL, a._airbyte_emitted_at from femora_mp_orders_meesho_swd a where \"Total Sales Amount (Incl. GST + Commission)\">=0 union all select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $49 from ( select \'TataCliq\', OrderID, NULL, customerid, transactionid, sku, SKU, sku.sku_id AS PRODUCT_ID ,sku.sku_name AS PRODUCT_NAME ,sku.category as PRODUCT_CATEGORY ,sku.\"Sub Category\" as PRODUCT_SUB_CATEGORY ,sku.mrp as PRODUCT_MRP, \'INR\', case when returnlogisticsid is not null then 1 else 0 end as IS_RETURN, shippingcity, upper(shippingstate), NULL, case when ReturnLogisticsID is not null then \'Returned\' else \'Ordered\' end, to_date(OrderDate,\'DD-MM-YY\'), coalesce(try_cast(price as float),0)+coalesce(try_cast(giftprice as float),0), try_cast(shippingcharge as float), NULL, NULL, try_cast(promotionvalue as float), coalesce(try_cast(Price as float),0)- coalesce(try_cast(PromotionValue as float),0), \'TataCliq\', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, try_cast(giftPrice as float), NULL, NULL, ShippingPincode, ShippingPhoneNo, ShippingEmailID, FulfillmentType, row_number()over(partition by OrderID order by asp._airbyte_emitted_at desc ) as rw, asp._airbyte_emitted_at from femora_mp_orders_tatacliq asp left join maplemonk.maplemonk.femora_sku_master sku on asp.sku=sku.sku_id where orderdate <>\'OrderDate\' ) a where rw = 1 union all select \'Myntra\', \"Seller Order Id\", NULL, NULL, \"Order Line Id\", \"Seller SKU Code\", \"Myntra SKU Code\", sku.sku_id AS PRODUCT_ID, sku.sku_name AS PRODUCT_NAME, sku.category as PRODUCT_CATEGORY, sku.\"Sub Category\" as PRODUCT_SUB_CATEGORY, sku.mrp as PRODUCT_MRP, \'INR\', case when \"Return Creation date\" is not null then 1 else 0 end IS_Return, city, UPPER(State), NULL, case when \"Order Status\" = \'C\' then \'Created\' when \"Order Status\" = \'DL\' then \'Delivered\' when \"Order Status\" = \'SH\' then \'Shipped\' when \"Order Status\" = \'RTO\' then \'Return\' when \"Order Status\" = \'F\' then \'Cancelled\' end, to_date(\"Order Date\",\'DD/MM/YYYY\'), coalesce(\"Final Amount\",0)+coalesce(\"Gift Charge\",0) +coalesce(DISCOUNT,0) + coalesce(\"Coupon Discount\",0), try_cast(\"Shipping Charge\" as float), NULL, NULL, coalesce(Discount,0)+coalesce(\"Coupon Discount\",0), \"Final Amount\", \'Myntra\', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \"Gift Charge\", NULL, NULL, zipcode, NULL, NULL, NULL, asp._airbyte_emitted_at from femora_mp_orders_myntra asp left join maplemonk.maplemonk.femora_sku_master sku on asp.\"Seller SKU Code\"=sku.sku_id; create or replace table sales_consolidated_intermediate_femora as select * from sales_consolidated_femora union select * from sales_consolidated_femora_final; create or replace table sales_consolidated_femora_final as select SHOP_NAME, ORDER_ID, ORDER_NAME, CUSTOMER_ID, LINE_ITEM_ID, SKU, MARKETPLACE_SKU_ID, PRODUCT_ID, PRODUCT_NAME, PRODUCT_CATEGORY, PRODUCT_SUB_CATEGORY, PRODUCT_MRP, CURRENCY, IS_RETURN, CITY, STATE, CATEGORY, ORDER_STATUS, ORDER_TIMESTAMP, LINE_ITEM_SALES, SHIPPING_PRICE, QUANTITY, TAX, DISCOUNT, NET_SALES, SOURCE, LANDING_UTM_MEDIUM, LANDING_UTM_SOURCE, LANDING_UTM_CAMPAIGN, REFERRING_UTM_MEDIUM, REFERRING_UTM_SOURCE, LANDING_UTM_CHANNEL, REFERRING_UTM_CHANNEL, FINAL_UTM_CHANNEL, CUSTOMER_FLAG, NEW_CUSTOMER_FLAG, ACQUISITION_CHANNEL, ACQUISITION_PRODUCT, SHIPPING_TAX, SHIP_PROMOTION_DISCOUNT, GIFT_WRAP_PRICE, GIFT_WRAP_TAX, TCS, PINCODE, CUSTOMER_PHONE, CUSTOMER_EMAIL, FULFILLMENT_TYPE, _airbyte_emitted_at from (select *, row_number()over(partition by order_id,coalesce(sku,\'1\') order by _airbyte_emitted_at desc) as rw from sales_consolidated_intermediate_femora ) where rw = 1;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MAPLEMONK.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        