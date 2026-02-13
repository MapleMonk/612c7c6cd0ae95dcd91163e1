{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.anveshan_amazon_vendor_purchase_orders; CREATE TABLE public.anveshan_amazon_vendor_purchase_orders as SELECT \'AMAZON VP\' as channel, t.purchaseordernumber, t.purchaseorderdate, t.purchaseordertype, t.purchaseorderstate, t.paymentmethod, t.deliverywindow, Upper(coalesce(p.CATEGORY,a.category)) AS Product_Category, Upper(coalesce(p.commonsku,a.commonsku)) AS commonsku, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(t.billtoparty), \'partyId\') as bill_to_party_id, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(t.shiptoparty), \'address\', \'addressLine1\') as ship_to_address, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(t.shiptoparty), \'address\', \'stateOrRegion\') as ship_to_city, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(t.shiptoparty), \'address\', \'postalCode\') as ship_to_pincode, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(t.buyingparty), \'partyId\') as buying_party_id, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'itemSequenceNumber\') as item_seq_no, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'amazonProductIdentifier\') as asin, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'vendorProductIdentifier\') as vendor_sku, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'orderedQuantity\', \'amount\')::FLOAT as ordered_qty, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'orderedQuantity\', \'unitOfMeasure\') as uom, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'netCost\', \'amount\')::DECIMAL(10,2) as net_cost, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'netCost\', \'currencyCode\') as currency FROM \"public\".\"amazon_vendor_anveshan_vendor_purchase_orders\" t LEFT JOIN t.items AS i ON TRUE left join (select * from ( select \"(child) asin\"::varchar as asin, category::varchar as category, sku::varchar as commonsku, upper(replace(lower(title::varchar),\'anveshan \',\'\')) as product_name, \"sub category\"::varchar as sub_category, row_number() over (partition by \"(child) asin\"::varchar,sku order by priority::varchar) r from public.anveshan_amazon_asin_to_sku_mapping ) where r = 1 ) a on lower(replace(replace((JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'amazonProductIdentifier\')),\' \',\'\'),\'\"\',\'\')) = lower(replace(a.asin,\' \',\'\')) LEFT JOIN ( SELECT * FROM ( SELECT master_sku as commonsku, amazon_sku as marketplace_sku, parent_category as category, tax_rate, product_name, parent_mrp, cogs, ROW_NUMBER() OVER (PARTITION BY amazon_sku ORDER BY LENGTH(COALESCE(amazon_sku, \'\')) DESC) rw FROM public.anveshan_sku_master ) WHERE rw = 1 ) p ON trim(LOWER(REPLACE(REPLACE(a.commonsku::varchar, \' \', \'\'),\'\"\',\'\'))) = trim(LOWER(p.marketplace_sku)) ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            