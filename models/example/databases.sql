{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.anveshan_amazon_vendor_purchase_orders; CREATE TABLE public.anveshan_amazon_vendor_purchase_orders as SELECT \'AMAZON VP\' as channel, t.purchaseordernumber, t.purchaseorderdate, t.purchaseordertype, t.purchaseorderstate, t.paymentmethod, t.deliverywindow, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(t.billtoparty), \'partyId\') as bill_to_party_id, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(t.shiptoparty), \'address\', \'addressLine1\') as ship_to_address, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(t.shiptoparty), \'address\', \'stateOrRegion\') as ship_to_city, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(t.shiptoparty), \'address\', \'postalCode\') as ship_to_pincode, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(t.buyingparty), \'partyId\') as buying_party_id, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'itemSequenceNumber\') as item_seq_no, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'amazonProductIdentifier\') as asin, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'vendorProductIdentifier\') as vendor_sku, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'orderedQuantity\', \'amount\')::FLOAT as ordered_qty, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'orderedQuantity\', \'unitOfMeasure\') as uom, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'netCost\', \'amount\')::DECIMAL(10,2) as net_cost, JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(i), \'netCost\', \'currencyCode\') as currency FROM \"public\".\"amazon_vendor_anveshan_vendor_purchase_orders\" t LEFT JOIN t.items AS i ON TRUE ;",
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
            