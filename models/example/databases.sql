{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create Or Replace Table zeproc_db.maplemonk.Zeproc_db_Magento_Sales_Fact_items as Select SOI.SKU, SOI.Name as Product_Name, SOI.PRODUCT_TYPE, SOI.ITEM_ID, SOI.PRODUCT_ID, SOI.QTY_ORDERED, SOI.QTY_SHIPPED, SOI.QTY_CANCELED, SOI.QTY_INVOICED, SOI.QTY_REFUNDED, SOI.Order_id, SOI.store_id, SOI.CREATED_AT::timestamp as CREATED_AT, SOI.UPDATED_AT , SOI.BASE_PRICE, SOI.PRICE_INCL_TAX, ifnull(SOI.PRICE_INCL_TAX,0) + ifnull(DIV0(ifnull(SO.BASE_SHIPPING_INCL_TAX,0),count(*) over(partition by soi.order_id)),0) as total_Sales, SO.INCREMENT_ID as Reference_code, SO.STATE, SO.STATUS, SO.STORE_CURRENCY_CODE, SO.STORE_NAME, SO.CUSTOMER_EMAIL, SO.CUSTOMER_FIRSTNAME, SO.CUSTOMER_LASTNAME, SO.CUSTOMER_MIDDLENAME, SO.SHIPPING_CANCELED, SO.SHIPPING_METHOD, sii.qty AS Sii_qty_invoiced, si.increment_id AS invoice_number, si.created_at AS invoice_date, scmi.qty AS scm_qty_refunded, ifnull(scmi.PRICE_INCL_TAX,0) + ifnull(DIV0(ifnull(SO.SHIPPING_TAX_REFUNDED,0),count(*) over(partition by soi.order_id)),0) + ifnull(DIV0(ifnull(SO.SHIPPING_REFUNDED,0),count(*) over(partition by soi.order_id)),0) as refund_price, scmi.row_total AS refund_row_total, scm.increment_id AS creditmemo_number, scm.created_at AS creditmemo_date, scm.state AS creditmemo_status, sop.method AS payment_method, sop.additional_information, sop.amount_paid, soa.firstname, soa.lastname, soa.street, soa.city, soa.region, soa.postcode, soa.country_id, soa.telephone from (select * from zeproc_db.maplemonk.zeproc_magento_sales_order_item where product_type = \'configurable\' )SOI LEFT JOIN( Select * from zeproc_db.maplemonk.zeproc_magento_sales_order Qualify row_number() over(partition by entity_id Order by 1) =1 )so ON soi.order_id = so.entity_id LEFT JOIN zeproc_db.maplemonk.zeproc_magento_sales_invoice AS si ON si.order_id = so.entity_id LEFT JOIN zeproc_db.maplemonk.zeproc_magento_sales_invoice_item AS sii ON sii.order_item_id = soi.item_id AND sii.parent_id = soi.order_id LEFT JOIN zeproc_db.maplemonk.zeproc_magento_sales_creditmemo_item AS scmi ON scmi.order_item_id = soi.item_id LEFT JOIN zeproc_db.maplemonk.zeproc_magento_sales_creditmemo AS scm ON scm.entity_id = scmi.parent_id LEFT JOIN zeproc_db.maplemonk.zeproc_magento_sales_order_payment AS sop ON sop.parent_id = soi.order_id LEFT JOIN zeproc_db.maplemonk.zeproc_magento_sales_order_address AS soa ON soa.parent_id = soi.order_id AND soa.address_type = \'shipping\' ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from ZEPROC_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            