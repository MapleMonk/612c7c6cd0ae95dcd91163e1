{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table rpsg_db.maplemonk.sales_w_last_order_details_drv as WITH LAST_ORDER AS ( select * from (select * ,row_number() over (partition by customer_id_final, date_trunc(\'month\',order_Date) order by order_Date desc) month_order_rank from (select order_id, reference_code, customer_id_final, email, phone, customer_name, order_date, final_channel, SKU, product_name_mapped, productname, report_category, category, row_number() over (partition by order_id order by selling_price desc) rw from RPSG_db.maplemonk.SALES_CONSOLIDATED_DRV ) where rw = 1 ) where month_order_rank = 1 ), sales_cte as ( select ORDER_DATE ,CUSTOMER_ID_FINAL ,CUSTOMER_NAME ,PHONE ,EMAIL ,ORDER_ID ,REFERENCE_CODE ,ORDER_STATUS ,SHIPPING_STATUS ,FINAL_STATUS ,FINAL_CHANNEL ,FINAL_UTM_CAMPAIGN ,MARKETPLACE ,LINE_ITEM_ID ,SKU ,PRODUCTNAME ,PRODUCT_NAME_MAPPED ,CATEGORY ,REPORT_CATEGORY ,sum(ifnull(SELLING_PRICE::float,0)) Booked_Revenue ,sum(ifnull(PRODUCT_QUANTITY,0)) Quantity from rpsg_db.maplemonk.SALES_CONSOLIDATED_DRV group by ORDER_DATE ,CUSTOMER_ID_FINAL ,CUSTOMER_NAME ,PHONE ,EMAIL ,ORDER_ID ,REFERENCE_CODE ,ORDER_STATUS ,SHIPPING_STATUS ,FINAL_STATUS ,FINAL_CHANNEL ,FINAL_UTM_CAMPAIGN ,MARKETPLACE ,LINE_ITEM_ID ,SKU ,PRODUCTNAME ,PRODUCT_NAME_MAPPED ,CATEGORY ,REPORT_CATEGORY ) select * from (select A.ORDER_DATE, B.ORDER_DATE last_order_month, A.CUSTOMER_ID_FINAL, A.CUSTOMER_NAME, A.PHONE, A.EMAIL, A.ORDER_ID, A.REFERENCE_CODE, A.ORDER_STATUS, A.SHIPPING_STATUS, A.FINAL_STATUS, A.FINAL_CHANNEL, A.FINAL_UTM_CAMPAIGN, A.MARKETPLACE, A.LINE_ITEM_ID, A.SKU, A.PRODUCTNAME, A.PRODUCT_NAME_MAPPED, A.CATEGORY, A.REPORT_CATEGORY, A.Booked_Revenue, A.QUANTITY, B.reference_code last_Order_reference_code, B.SKU last_order_SKU, B.product_name_mapped last_order_product_name_mapped, B.productname last_order_productname, B.report_category last_order_report_category, B.category last_order_category, B.final_channel last_order_final_channel, row_number() over (partition by A.order_id, A.line_item_id order by date_trunc(\'month\',B.ORDER_DATE) desc) rw from sales_cte as A LEFT JOIN LAST_ORDER B ON A.CUSTOMER_ID_FINAL = B.CUSTOMER_ID_FINAL AND date_trunc(\'month\', A.ORDER_DATE) > date_trunc(\'month\',B.ORDER_DATE) ) where rw=1;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from RPSG_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        