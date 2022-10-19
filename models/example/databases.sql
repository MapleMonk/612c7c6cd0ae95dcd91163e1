{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table welspun_db.maplemonk.asp_usa_orders_ws as select _AIRBYTE_UNIQUE_KEY, ISISPU, ISPRIME, ORDERTYPE, SELLER_ID, ISSOLDBYAB, replace(amount,\'\"\',\'\')::float as amount, replace(currency,\'\"\',\'\') as currency, ORDERSTATUS, PURCHASEDATE, SALESCHANNEL, AMAZONORDERID, MARKETPLACEID, PAYMENTMETHOD, SELLERORDERID, ISPREMIUMORDER, LASTUPDATEDATE, LATESTSHIPDATE, ISBUSINESSORDER, EARLIESTSHIPDATE, SHIPSERVICELEVEL, FULFILLMENTCHANNEL, ISREPLACEMENTORDER, NUMBEROFITEMSSHIPPED, PAYMENTMETHODDETAILS, ISGLOBALEXPRESSENABLED, NUMBEROFITEMSUNSHIPPED, SHIPMENTSERVICELEVELCATEGORY from ( select _AIRBYTE_UNIQUE_KEY, ISISPU, ISPRIME, ORDERTYPE, SELLER_ID, ISSOLDBYAB, ordertotal:\"Amount\" as amount, ordertotal:\"CurrencyCode\" as currency, ORDERSTATUS, PURCHASEDATE, SALESCHANNEL, AMAZONORDERID, MARKETPLACEID, PAYMENTMETHOD, SELLERORDERID, ISPREMIUMORDER, LASTUPDATEDATE, LATESTSHIPDATE, ISBUSINESSORDER, EARLIESTSHIPDATE, SHIPSERVICELEVEL, FULFILLMENTCHANNEL, ISREPLACEMENTORDER, NUMBEROFITEMSSHIPPED, PAYMENTMETHODDETAILS, ISGLOBALEXPRESSENABLED, NUMBEROFITEMSUNSHIPPED, SHIPMENTSERVICELEVELCATEGORY, _AIRBYTE_AB_ID, _AIRBYTE_EMITTED_AT, _AIRBYTE_NORMALIZED_AT, _AIRBYTE_ASP_USA_ORDERS_HASHID from welspun_db.maplemonk.asp_usa_orders ); CREATE OR REPLACE TABLE WELSPUN_DB.MAPLEMONK.asp_usa_get_flat_file_all_orders_data_by_last_update_generaL_WELSPUN AS SELECT \"_AIRBYTE_UNIQUE_KEY\" as unique_key, \"SKU\", \"ASIN\", \"CURRENCY\", try_cast(\"item-tax\" as float) as item_tax, \"QUANTITY\"::float as quantity, \"ship-city\" as ship_city, try_cast(\"item-price\" as float) as item_price, upper(\"ship-state\") as ship_state, \"item-status\" as item_status, \"order-status\" as order_status, \"product-name\" as product_name, \"ship-country\" as ship_country, try_cast(\"shipping-tax\" as float) as shipping_tax, try_cast(\"gift-wrap-tax\" as float) as gift_wrap_tax, \"order-channel\" as order_channel, \"promotion-ids\" as promotion_ids, to_timestamp(\"purchase-date\") as purchase_date, \"sales-channel\" as sales_channel, try_cast(\"shipping-price\" as float) as shipping_price, \"amazon-order-id\" as amazon_order_id, try_cast(\"gift-wrap-price\" as float) as gift_wrap_price, \"ship-postal-code\" as ship_postal_code, \"is-business-order\" as is_business_order, to_timestamp(\"last-updated-date\") as last_update_date, \"merchant-order-id\" as merchant_order_id, \"price-designation\" as price_designation, \"ship-service-level\" as ship_service_level, \"fulfillment-channel\" as fulfilment_channel, \"purchase-order-number\" as purchase_order_number, try_cast(\"item-promotion-discount\" as float) as item_promotion_discount, try_cast(\"ship-promotion-discount\" as float) AS ship_promotion_discount FROM WELSPUN_DB.MAPLEMONK.asp_usa_get_flat_file_all_orders_data_by_last_update_general; create or replace table welspun_db.maplemonk.asp_usa_get_fba_fulfillment_customer_returns_data_welspun as select \"SKU\", \"ASIN\", \"FNSKU\", \"REASON\", \"STATUS\", \"order-id\" as order_id, try_cast(\"QUANTITY\" as float) as quantity, to_timestamp(\"return-date\") as return_date, \"product-name\" as product_name, \"customer-comments\" as customer_comments, \"detailed-disposition\" as detailed_disposition, \"license-plate-number\" as license_plate_number, \"fulfillment-center-id\" as fulfillment_center_id from welspun_db.maplemonk.asp_usa_get_fba_fulfillment_customer_returns_data; create or replace table WELSPUN_DB.MAPLEMONK.asp_usa_get_v2_settlement_report_data_flat_file_welspun as select \"SKU\", \"CURRENCY\", \"order-id\" as order_id, \"REPORT_ID\" as report_id, \"price-type\" as price_type, try_to_timestamp(\"posted-date\") as posted_date, \"shipment-id\" as shipment_id, try_to_timestamp(\"deposit-date\") as deposit_date, try_cast(\"other-amount\" as float) as other_amount, try_cast(\"price-amount\" as float) as price_amount, \"promotion-id\" as promotion_id, try_cast(\"total-amount\" as float) as total_amount, \"adjustment-id\" as adjustment_id, \"settlement-id\" as settlement_id, \"fulfillment-id\" as fulfilment_id, \"order-fee-type\" as order_fee_type, \"promotion-type\" as promotion_type, try_cast(\"misc-fee-amount\" as float) as misc_fee_amount, \"order-item-code\" as order_item_code, \"marketplace-name\" as marketplace_name, try_cast(\"order-fee-amount\" as float) as order_fee_amount, try_cast(\"other-fee-amount\" as float)as other_fee_amount, try_cast(\"promotion-amount\" as float) as promotion_amount, \"transaction-type\" as transaction_type, \"merchant-order-id\" as merchant_order_id, \"shipment-fee-type\" as shipment_fee_type, try_cast(\"quantity-purchased\" as float) as quantity_processed, \"direct-payment-type\" as direct_payment_type, \"settlement-end-date\" as settlement_end_date, try_cast(\"shipment-fee-amount\" as float) as shipment_fee_amount, try_cast(\"direct-payment-amount\" as float) as direct_payment_amount, \"item-related-fee-type\" as item_related_fee_type, try_to_timestamp(\"settlement-start-date\") as settlement_start_date, \"merchant-order-item-id\" as merchant_order_item_id, try_cast(\"item-related-fee-amount\" as float) as item_related_fee_amount, \"merchant-adjustment-item-id\" as merchant_adjustment_item_id, \"other-fee-reason-description\" as other_fee_reason_description from WELSPUN_DB.MAPLEMONK.asp_usa_get_v2_settlement_report_data_flat_file; create or replace table welspun_db.maplemonk.asp_usa_get_fba_storage_fee_charges_data_welspun as select \"ASIN\", \"FNSKU\", try_cast(\"WEIGHT\" as float) WEIGHT, \"CURRENCY\", try_cast(\"item-volume\" as float) as item_volume, \"median-side\" as median_side, \"country-code\" as country_code, try_cast(\"longest-side\" as float) as longest_side, \"product-name\" as product_name, try_cast(\"storage-rate\" as float) as storage_rate, try_cast(\"volume-units\" as float) as volume_units, try_cast(\"weight-units\" as float) as weight_units, try_cast(\"shortest-side\" as float) as shortest_side, \"month-of-charge\" as month_of_charge, try_cast(\"measurement-units\" as float) as measurement_units, \"product-size-tier\" as products_size_tier, \"fulfillment-center\" as fulfillment_center, try_cast(\"average-quantity-on-hand\" as float) as average_quantity_on_hand, try_cast(\"estimated-total-item-volume\" as float) as estimated_total_item_volume, try_cast(\"estimated-monthly-storage-fee\" as float) as estimated_monthly_shortage_fee, try_cast(\"average-quantity-pending-removal\" as float) as average_quantity_pending_approval from welspun_db.maplemonk.asp_usa_get_fba_storage_fee_charges_data; create or replace table welspun_db.maplemonk.asp_usa_get_fba_inventory_aged_data_welspun as select \"SKU\", \"ASIN\", \"FNSKU\", \"CONDITION\", try_cast(\"your-price\" as float ) as your_price, \"product-name\" product_name, try_cast(\"per-unit-volume\" as float) as per_unit_volume, \"afn-listing-exists\" as afn_listing_exists, try_cast(\"afn-total-quantity\" as float) as afn_total_quantity, \"mfn-listing-exists\" as mfn_listing_exists, try_cast(\"afn-reserved-quantity\" as float) as afn_reserved_quantity, try_cast(\"afn-warehouse-quantity\" as float) afn_warehouse_quantity , try_cast(\"afn-unsellable-quantity\" as float) as afn_unsellable_quantity, try_cast(\"afn-fulfillable-quantity\" as float) as unfulfillable_quantity, try_cast(\"mfn-fulfillable-quantity\" as float) as mfn_unfulfillable_quantity, try_cast(\"afn-future-supply-buyable\" as float) as afn_future_supply_buyable , try_cast(\"afn-reserved-future-supply\" as float) as afn_reserved_future_supply, try_cast(\"afn-inbound-shipped-quantity\" as float ) as afn_inbound_shipped_quantity, try_cast(\"afn-inbound-working-quantity\" as float) as afn_inbound_working_quantity, try_cast(\"afn-inbound-receiving-quantity\" as float) as afn_inbound_receiving_quantity from welspun_db.maplemonk.asp_usa_get_fba_inventory_aged_data; create or replace table WELSPUN_DB.MAPLEMONK.asp_usa_get_fba_fulfillment_customer_shipment_replacement_data_welspun as select \"SKU\", \"ASIN\", try_cast(\"QUANTITY\" as float) as quantity, to_timestamp(\"shipment-date\") as shipment_date, \"fulfillment-center-id\" as fulfillment_center_id, \"replacement-reason-code\" as replacement_reason_code, \"original-amazon-order-id\" as original_amazon_order_id, \"replacement-customer-order-id\" as replacement_customer_order_id, \"original-fulfillment-center-id\" as original_fulfillment_center_id from WELSPUN_DB.MAPLEMONK.asp_usa_get_fba_fulfillment_customer_shipment_replacement_data; create or replace table welspun_db.maplemonk.asp_usa_get_restock_inventory_recommendations_report_welspun as select \"ASIN\", \"ALERT\", \"FNSKU\", try_Cast(\"PRICE\" as float) as price, \"COUNTRY\", try_cast(\"INBOUND\" as float )as inbound, try_cast(\"SHIPPED\" as float) as shipped, try_cast(\"WORKING\" as float) as working, \"SUPPLIER\", try_cast(\"AVAILABLE\" as float) as available, \"CONDITION\", try_cast(\"RECEIVING\" as float )as receiving, try_Cast(\"FC transfer\" as float) as fc_transfer, try_cast(\"Total Units\" as float) as total_units, \"Fulfilled by\" as fulfilled_by, \"Merchant SKU\" as Merchant_SKU, \"Product Name\" as product_name, \"Currency code\" as currency_code, \"FC Processing\" as FC_processing, \"UNFULFILLABLE\", \"Customer Order\" as customer_order, \"Supplier part no.\" as supplier_part_no, \"Recommended action\" as recommended_action, try_cast(\"Sales last 30 days\" as float) as sales_last30_days, try_to_timestamp(\"Recommended ship date\") as recommended_ship_date, try_cast(\"Units Sold Last 30 Days\" as float) as units_sold_last30_days, try_cast(\"Recommended replenishment qty\" as float) as recommended_replennishment_qty, try_cast(\"Days of Supply at Amazon Fulfillment Network\" as number) as DOS_at_amazon_fulfillment_network, try_cast(\"Total Days of Supply (including units from open shipments)\" as number) as total_dos_inclopenshipments from welspun_db.maplemonk.asp_usa_get_restock_inventory_recommendations_report; create or replace table welspun_db.maplemonk.ASP_Consolidated_welspun as with cte as ( with returns as ( select order_id,asin,max(return_date) return_date ,sum(quantity) as quantity from welspun_db.maplemonk.asp_usa_get_fba_fulfillment_customer_returns_data_welspun group by 1,2 order by 4 desc ), replacements as ( select original_amazon_order_id,asin,max(shipment_date) as replacement_date,sum(quantity) as quantity from welspun_db.maplemonk.asp_usa_get_fba_fulfillment_customer_shipment_replacement_data_welspun group by 1,2 ) select a.*,b.return_date as return_date,b.quantity as return_quantity,c.replacement_date as replacement_date,c.quantity as replacement_quantity from welspun_db.maplemonk.asp_usa_get_flat_file_all_orders_data_by_last_update_general_welspun a left join returns b on a.amazon_order_id= b.order_id and a.asin=b.asin left join replacements c on a.amazon_order_id=c.original_amazon_order_id and a.asin=c.asin ) select * , (item_price/quantity)*return_quantity as return_value from cte;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from WELSPUN_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        