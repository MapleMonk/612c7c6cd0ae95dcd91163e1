{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.ECO_Pendency AS SELECT e.AWB, e.RAN, e.SKU, e.STATUS, e.COURIER, e.ORDER_ID, e.QUANTITY, date_trunc(\'Day\',e.CREATED_AT::date) as Created_at , date_trunc(\'Day\',e.PICKUP_DATE::date) as PICKUP_DATE, e.REQUEST_TYPE, e.REFUND_STATUS, e.REFUNDED_DATE, e.PAYMENT_METHOD, e.RETURN_REASONS, e.sub_reason_detail, e.exchange_order_number, e.exchange_item_sku, e.exchange_created_date, CASE WHEN e.STATUS = \'OPEN\' THEN \'Open\' WHEN e.STATUS IN (\'Cancelled\', \'Cancellation Requested\', \'Canceled\') THEN \'Cancelled\' WHEN e.STATUS = \'FAILED\' THEN \'Failed\' WHEN e.STATUS IN (\'OutForPickup\', \'PICKUP CREATED\', \'Pickup Exception\', \'CREATING PICKUP\', \'PickupFailed\', \'Out for Pickup\', \'Pickup Generated\', \'PickupPending\') THEN \'Not Picked Up\' WHEN e.STATUS IN (\'Delivered\', \'OutForDelivery\', \'Refunded\', \'PickedUp\', \'In Transit\', \'InTransit\', \'Payout_Link_Sent\', \'Picked Up\', \'Out for Delivery\', \'Shipped\', \'Lost\', \'Received\',\'PICKED UP\') THEN \'Picked Up\' WHEN e.STATUS = \'REJECTED\' THEN \'REJECTED\' WHEN e.STATUS = \'exchange_created\' THEN CASE WHEN t.shippingpackagestatus IN (\'DELIVERED\', \'SHIPPED\', \'PICKING\',\'READY_TO_SHIP\',\'RETURNED\',\'CREATED\') THEN \'Picked Up\' ELSE \'Not Picked Up\' END END AS Final_Status, CASE WHEN lower(e.request_type) = \'exchange\' THEN \'Exchange\' WHEN e.REFUND_STATUS LIKE \'%refund%\' THEN \'Refunded\' WHEN e.STATUS IN (\'Delivered\', \'OutForDelivery\', \'Refunded\', \'PickedUp\', \'In Transit\', \'InTransit\', \'Payout_Link_Sent\', \'Picked Up\', \'Out for Delivery\', \'Shipped\', \'Lost\', \'Received\',\'PICKED UP\') THEN \' Refund Pending\' When e.STATUS NOT IN (\'Delivered\', \'OutForDelivery\', \'Refunded\', \'PickedUp\', \'In Transit\', \'InTransit\', \'Payout_Link_Sent\', \'Picked Up\', \'Out for Delivery\', \'Shipped\', \'Lost\', \'Received\',\'PICKED UP\') and e.refund_status LIKE \'%refund%\' then \'Not Picked But Refunded(Maunual Refund)\' ELSE Final_Status END AS Final_Refund_Status, t.shippingpackagecode, t.shippingpackagestatus, t.order_name AS t_order_name, t.sku AS t_sku, t.AWB AS \"UC AWB\", sc.awb_number AS \"CP AWB\", sc.current_status AS \"CP Status\" FROM snitch_db.maplemonk.ecoreturns_final_returns e LEFT JOIN snitch_db.maplemonk.unicommerce_fact_items_intermediate t ON CONCAT(e.exchange_order_number, e.exchange_item_sku) = CONCAT(t.ORDER_name, t.sku) LEFT JOIN snitch_db.snitch.sleepycat_db_clickpost_fact_items sc ON t.AWB = sc.awb_number",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        